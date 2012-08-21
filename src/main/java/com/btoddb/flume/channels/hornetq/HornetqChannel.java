package com.btoddb.flume.channels.hornetq;

import java.lang.management.ManagementFactory;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.apache.flume.ChannelException;
import org.apache.flume.Context;
import org.apache.flume.channel.BasicChannelSemantics;
import org.apache.flume.channel.BasicTransactionSemantics;
import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.api.core.client.HornetQClient;
import org.hornetq.api.core.client.ServerLocator;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.config.impl.ConfigurationImpl;
import org.hornetq.core.remoting.impl.invm.InVMAcceptorFactory;
import org.hornetq.core.remoting.impl.invm.InVMConnectorFactory;
import org.hornetq.core.server.JournalType;
import org.hornetq.core.server.embedded.EmbeddedHornetQ;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.btoddb.flume.sinks.cassandra.JmxStatsHelper;

public class HornetqChannel extends BasicChannelSemantics implements HornetqChannelMXBean {
    private static final Logger logger = LoggerFactory.getLogger(HornetqChannel.class);

    private static final String MBEAN_NAME_ROOT = "com.btoddb.flume.channels.hornetq.HornetqChannel:type=";

    public static final String STAT_PUT = "put";
    public static final String STAT_TAKE = "take";
    public static final String STAT_CHANNEL_SIZE = "size";
    public static final String STAT_ROLLBACKS = "rollbacks";
    public static final String STAT_COMMITS = "commits";

    private static final String DEFAULT_DATA_DIR = "./hornetq-data-dir";
    private static final long DEFAULT_MAX_CHANNEL_SIZE = 1000000;
    private static final long DEFAULT_MAX_PUT_WAIT_TIME = 1000;

    private static EmbeddedHornetQ server;
    private static Object serverCreateMonitor = new Object();

    private MBeanServer mbs;
    private JmxStatsHelper stats;

    private ServerLocator locator;
    private ClientSessionFactory factory;

    private String queueName;
    private long maxChannelSize;
    private long maxPutWaitTime;
    private String dataDir;

    @Override
    protected BasicTransactionSemantics createTransaction() {
        ClientSession session;
        try {
            session = factory.createTransactedSession();
        }
        catch (HornetQException e) {
            throw new ChannelException("exception while creating hornetq session", e);
        }
        return new HornetqTransaction(stats, queueName, session, maxChannelSize, maxPutWaitTime);
    }

    @Override
    public void configure(Context context) {
        super.configure(context);

        if (null != context) {
            dataDir = context.getString("dataDir", DEFAULT_DATA_DIR);
            maxChannelSize = context.getLong("maxChannelSize", DEFAULT_MAX_CHANNEL_SIZE);
            maxPutWaitTime = context.getLong("maxPutWaitTime", DEFAULT_MAX_PUT_WAIT_TIME);
            queueName = context.getString("queueName", getName());
        }

        if (null == stats) {
            stats = new JmxStatsHelper(5 * 1000);
        }

        mbs = ManagementFactory.getPlatformMBeanServer();
        try {
            mbs.registerMBean(this, new ObjectName(MBEAN_NAME_ROOT + getQueueName()));
        }
        catch (Throwable e) {
            logger.error("exception while registering me as mbean, " + MBEAN_NAME_ROOT + getName(), e);
        }
    }

    @Override
    protected void initialize() {
        createHornetqServer();

        try {
            locator = HornetQClient.createServerLocatorWithoutHA(new TransportConfiguration(InVMConnectorFactory.class
                    .getName()));

            factory = locator.createSessionFactory();

            // create queue if doesn't exist
            ClientSession session = null;
            try {
                session = factory.createSession(false, false, false);
                session.createQueue(queueName, queueName, true);
                session.close();
            }
            catch (HornetQException e) {
                if (e.getCode() != HornetQException.QUEUE_EXISTS) {
                    throw new ChannelException("exception while creating hornetq session", e);
                }
            }
            finally {
                if (null != session) {
                    session.close();
                }
            }
        }
        catch (Throwable e) {
            logger.error("exception while configuring hornetq", e);
            throw new ChannelException("exception while configuring hornetq", e);
        }

    }

    private void createHornetqServer() {
        synchronized (serverCreateMonitor) {
            if (null != server) {
                return;
            }

            Configuration config = new ConfigurationImpl();
            config.setBindingsDirectory(dataDir+"/bindings");
            config.setJournalType(JournalType.NIO); // TODO:BTB use AIO on linux
            config.setJournalDirectory(dataDir+"/journal");
            config.setLargeMessagesDirectory(dataDir+"/large");
            config.setSecurityEnabled(false);

            config.getAcceptorConfigurations().add(new TransportConfiguration(InVMAcceptorFactory.class.getName()));

            server = new EmbeddedHornetQ();
            server.setMbeanServer(mbs);
            server.setConfiguration(config);
            try {
                server.start();
            }
            catch (Exception e) {
                logger.error("exception while creating hornetq server", e);
                throw new ChannelException("exception while configuring hornetq", e);
            }
        }

    }

    @Override
    public synchronized void stop() {
        super.stop();

        try {
            server.stop();
        }
        catch (Throwable e) {
            throw new ChannelException("exception while closing queue", e);
        }
    }

    @Override
    public int getPutAvgInMicros() {
        return stats.getRollingStat(STAT_PUT).getAverageAmount();
    }

    @Override
    public int getPutsPerSecond() {
        return stats.getRollingStat(STAT_PUT).getCountPerSecond();
    }

    @Override
    public int getTakeAvgInMicros() {
        return stats.getRollingStat(STAT_TAKE).getAverageAmount();
    }

    @Override
    public int getTakesPerSecond() {
        return stats.getRollingStat(STAT_TAKE).getCountPerSecond();
    }

    @Override
    public long getChannelSize() {
        return stats.getCounterStat(STAT_CHANNEL_SIZE);
    }

    @Override
    public long getNumberRollbacks() {
        return stats.getCounterStat(STAT_ROLLBACKS);
    }

    @Override
    public long getNumberCommits() {
        return stats.getCounterStat(STAT_COMMITS);
    }

    @Override
    public long getMaxPutWaitTime() {
        return maxPutWaitTime;
    }

    @Override
    public void setMaxPutWaitTime(long maxPutWaitTime) {
        this.maxPutWaitTime = maxPutWaitTime;
    }

    @Override
    public long getMaxChannelSize() {
        return maxChannelSize;
    }

    @Override
    public void setMaxChannelSize(long maxChannelSize) {
        this.maxChannelSize = maxChannelSize;
    }

    @Override
    public String getQueueName() {
        return queueName;
    }

    public void setQueueName(String queueName) {
        this.queueName = queueName;
    }

    public String getDataDir() {
        return dataDir;
    }

    public void setDataDir(String dataDir) {
        this.dataDir = dataDir;
    }

}
