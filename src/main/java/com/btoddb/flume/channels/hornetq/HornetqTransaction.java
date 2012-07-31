package com.btoddb.flume.channels.hornetq;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.flume.ChannelException;
import org.apache.flume.Event;
import org.apache.flume.channel.BasicTransactionSemantics;
import org.hornetq.api.core.HornetQBuffer;
import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.client.ClientConsumer;
import org.hornetq.api.core.client.ClientMessage;
import org.hornetq.api.core.client.ClientProducer;
import org.hornetq.api.core.client.ClientSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.btoddb.flume.channels.FlumeEvent;
import com.btoddb.flume.sinks.cassandra.JmxStatsHelper;

public class HornetqTransaction extends BasicTransactionSemantics {
    private static final Logger logger = LoggerFactory.getLogger(HornetqTransaction.class);

    private JmxStatsHelper stats;

    private String queueName;
    private ClientSession session;
    private ClientProducer producer;
    private ClientConsumer consumer;

    private long count;
    private long maxChannelSize;
    private long maxPutWaitTime;

    public HornetqTransaction(JmxStatsHelper stats, String queueName, ClientSession session, long maxChannelSize,
            long maxPutWaitTime) {
        this.stats = stats;

        this.queueName = queueName;
        this.session = session;

        this.maxChannelSize = maxChannelSize;
        this.maxPutWaitTime = maxPutWaitTime;
    }

    @Override
    protected void doPut(Event event) throws InterruptedException {
        long statStartTime = System.nanoTime();

        if (null == producer) {
            if (null != consumer) {
                throw new ChannelException(
                        "This transaction has already been used for 'take'.  Cannot mix 'put' and 'take' on same transaction");
            }

            try {
                producer = session.createProducer(queueName);
            }
            catch (HornetQException e) {
                throw new ChannelException("exception while createing hornetq producer", e);
            }
        }

        try {
            FlumeEvent flumeEvent = new FlumeEvent(event.getHeaders(), event.getBody());
            ByteArrayOutputStream byteOutput = new ByteArrayOutputStream(512);
            DataOutputStream dataOutput = new DataOutputStream(byteOutput);
            try {
                flumeEvent.write(dataOutput);
                dataOutput.flush();
            }
            catch (IOException e) {
                throw new ChannelException("Put failed because of I/O issue", e);
            }

            ClientMessage message = session.createMessage(true);
            message.getBodyBuffer().writeBytes(byteOutput.toByteArray());

            // i don't care if the count goes over for a bit
            long startWaitTime = System.currentTimeMillis();
            while (stats.getCounterStat(HornetqChannel.STAT_CHANNEL_SIZE) >= maxChannelSize
                    && (System.currentTimeMillis() - startWaitTime) > maxPutWaitTime) {
                Thread.sleep(5);
            }

            if (stats.getCounterStat(HornetqChannel.STAT_CHANNEL_SIZE) < maxChannelSize) {
                producer.send(message);
                stats.incrementCounter(HornetqChannel.STAT_CHANNEL_SIZE, 1);
                count++;
            }
            else {
                throw new ChannelException("Channel max of " + maxChannelSize
                        + " has been reached - 'take'ers are not keeping up");
            }
        }
        catch (HornetQException e) {
            throw new ChannelException("exception while putting event on queue", e);
        }
        finally {
            long duration = (System.nanoTime() - statStartTime) / 1000;
            logger.debug("put duration (micros) = {}", duration);
            stats.updateRollingStat(HornetqChannel.STAT_PUT, 1, duration);
        }
    }

    @Override
    protected Event doTake() throws InterruptedException {
        long statStartTime = System.nanoTime();

        if (null == consumer) {
            if (null != producer) {
                throw new ChannelException(
                        "This transaction has already been used for 'put'.  Cannot mix 'put' and 'take' on same transaction");
            }

            try {
                session.start();
                consumer = session.createConsumer(queueName);
            }
            catch (HornetQException e) {
                throw new ChannelException("exception while createing hornetq consumer", e);
            }
        }

        try {
            final ClientMessage msg = consumer.receive(10);
            if (null == msg) {
                return null;
            }
            
            msg.acknowledge();
            Event event = readEvent(msg);
            if (null != event) {
                stats.incrementCounter(HornetqChannel.STAT_CHANNEL_SIZE, -1);
                count--;
                return event;
            }
            return null;
        }
        catch (HornetQException e) {
            throw new ChannelException("exception while consuming event from hornetq", e);
        }
        finally {
            long duration = (System.nanoTime() - statStartTime) / 1000;
            logger.debug("take duration (micros) = {}", duration);
            stats.updateRollingStat(HornetqChannel.STAT_TAKE, 1, duration);
        }
    }

    private Event readEvent(final ClientMessage msg) {
        InputStream is = new InputStream() {
            @Override
            public int read() throws IOException {
                HornetQBuffer buf = msg.getBodyBuffer();
                byte b = buf.readByte();
                return b & 0xff;
            }
        };
        DataInputStream in = new DataInputStream(is);
        FlumeEvent event = null;
        try {
            event = FlumeEvent.from(in);
        }
        catch (IOException e) {
            logger.error("could not deserialize 'Event' object - trashing and moving on", e);
        }
        return event;
    }

    @Override
    protected void doCommit() throws InterruptedException {
        try {
            session.commit();
            session.close();
        }
        catch (HornetQException e) {
            throw new ChannelException("exception while commiting hornetq session", e);
        }

        cleanup();

        if (0 != count) {
            stats.incrementCounter(HornetqChannel.STAT_COMMITS, 1);
        }
    }

    @Override
    protected void doRollback() throws InterruptedException {
        try {
            session.rollback();
            session.close();
        }
        catch (HornetQException e) {
            throw new ChannelException("exception while rolling back hornetq session", e);
        }

        cleanup();

        stats.incrementCounter(HornetqChannel.STAT_ROLLBACKS, 1);
        if (0 != count) {
            stats.incrementCounter(HornetqChannel.STAT_CHANNEL_SIZE, -count);
        }
    }

    private void cleanup() {
        if (null != consumer) {
            try {
                consumer.close();
            }
            catch (HornetQException e) {
                logger.error("exception while closing hornetq consumer", e);
            }
        }
        if (null != producer) {
            try {
                producer.close();
            }
            catch (HornetQException e) {
                logger.error("exception while closing hornetq producer", e);
            }
        }
    }
}
