package com.btoddb.flume.channels.berkeley;

import java.io.File;
import java.lang.management.ManagementFactory;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.apache.flume.ChannelException;
import org.apache.flume.Context;
import org.apache.flume.channel.BasicChannelSemantics;
import org.apache.flume.channel.BasicTransactionSemantics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.btoddb.flume.sinks.cassandra.JmxStatsHelper;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Durability;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;

public class BerkeleyChannel extends BasicChannelSemantics implements BerkeleyChannelMXBean {
    private static final Logger logger = LoggerFactory.getLogger(BerkeleyChannel.class);

    private static final String MBEAN_NAME_ROOT = "com.btoddb.flume.channels.BerkeleyChannel:type=";
    
    public static final String STAT_PUT = "put";
    public static final String STAT_TAKE = "take";
    public static final String STAT_CHANNEL_SIZE = "size";
    public static final String STAT_ROLLBACKS = "rollbacks";
    public static final String STAT_COMMITS = "commits";

    private static final String DEFAULT_TIMESTAMP_HEADER = "timestamp";
    private static final String DEFAULT_DB_PATH = "flume-berkeley-channel";
    private static final long DEFAULT_MAX_CHANNEL_SIZE = 1000000;
    private static final long DEFAULT_MAX_PUT_WAIT_TIME = 1000;

    private MBeanServer mbs;
    private JmxStatsHelper stats;

    private Environment dbEnv = null;
    private File dbPath = new File("./flume-berkley-db");
    private Database db = null;
    private String timestampHeader;
    private long maxChannelSize;
    private long maxPutWaitTime;

    @Override
    protected BasicTransactionSemantics createTransaction() {
        return new BerkeleyTransaction(stats, dbEnv, db, timestampHeader, maxChannelSize, maxPutWaitTime);
    }

    @Override
    public void configure(Context context) {
        super.configure(context);
        timestampHeader = context.getString("timestampHeader", DEFAULT_TIMESTAMP_HEADER);
        maxChannelSize = context.getLong("maxChannelSize", DEFAULT_MAX_CHANNEL_SIZE);
        maxPutWaitTime = context.getLong("maxPutWaitTime", DEFAULT_MAX_PUT_WAIT_TIME);

        dbPath = new File(context.getString("dbPath", DEFAULT_DB_PATH));
        if (dbPath.exists()) {
            if (!dbPath.isDirectory()) {
                throw new ChannelException("dbPath, " + dbPath.getAbsolutePath()
                        + ", is not a directory - cannot continue");
            }
        }
        else if (!dbPath.mkdir()) {
            throw new ChannelException("dbPath, " + dbPath.getAbsolutePath() + ", is not a directory - cannot continue");
        }
        
        if (null == stats) {
            stats = new JmxStatsHelper(5 * 1000);
        }

        mbs = ManagementFactory.getPlatformMBeanServer();
        try {
            mbs.registerMBean(this, new ObjectName(MBEAN_NAME_ROOT + getName()));
        }
        catch (Throwable e) {
            logger.error("exception while registering me as mbean, " + MBEAN_NAME_ROOT + getName(), e);
        }
    }

    @Override
    protected void initialize() {
        try {
            EnvironmentConfig envConfig = new EnvironmentConfig();
            envConfig.setAllowCreate(true);
            envConfig.setTransactional(true);
            envConfig.setDurability(Durability.COMMIT_WRITE_NO_SYNC);

            dbEnv = new Environment(dbPath, envConfig);

            DatabaseConfig dbConfig = new DatabaseConfig();
            dbConfig.setAllowCreate(true);
            dbConfig.setTransactional(true);
            dbConfig.setSortedDuplicates(true);

            // TODO:possibly set db comparator for "long" type if perf not good
            db = dbEnv.openDatabase(null, "flume-channel", dbConfig);
        }
        catch (DatabaseException e) {
            throw new ChannelException("Exception while configuring database", e);
        }
    }

    @Override
    public synchronized void stop() {
        super.stop();

        try {
            if (db != null) {
                db.close();
            }
        }
        catch (DatabaseException e) {
            throw new ChannelException("exception while closing database", e);
        }

        try {
            if (dbEnv != null) {
                dbEnv.close();
            }
        }
        catch (DatabaseException e) {
            throw new ChannelException("exception while closing database", e);
        }
    }

    public void setTimestampHeader(String timestampHeader) {
        this.timestampHeader = timestampHeader;
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

}
