package com.btoddb.flume.channels.berkeley;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

import org.apache.flume.ChannelException;
import org.apache.flume.Event;
import org.apache.flume.channel.BasicTransactionSemantics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.btoddb.flume.channels.FlumeEvent;
import com.btoddb.flume.sinks.cassandra.JmxStatsHelper;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.TransactionConfig;

public class BerkeleyTransaction extends BasicTransactionSemantics {
    private static final Logger logger = LoggerFactory.getLogger(BerkeleyTransaction.class);

    private JmxStatsHelper stats;
    private Environment dbEnv;
    private final Database db;
    private final Transaction txn;
    private Cursor cursor;
    private boolean putMode = false;
    private String timestampHeader;
    private long count;
    private long maxChannelSize;
    private long maxPutWaitTime;

    public BerkeleyTransaction(JmxStatsHelper stats, Environment dbEnv, Database db, String timestampHeader,
            long maxChannelSize, long maxPutWaitTime) {
        this.stats = stats;
        this.dbEnv = dbEnv;
        this.db = db;
        this.timestampHeader = timestampHeader;
        this.maxChannelSize = maxChannelSize;
        this.maxPutWaitTime = maxPutWaitTime;

        TransactionConfig txnConfig = new TransactionConfig();
        txnConfig.setReadCommitted(true);
        this.txn = this.dbEnv.beginTransaction(null, txnConfig);
    }

    @Override
    protected void doPut(Event event) throws InterruptedException {
        if (null != cursor) {
            throw new ChannelException(
                    "This transaction has already been used for 'take'.  Cannot mix 'put' and 'take' on same transaction");
        }

        long statStartTime = System.nanoTime();

        try {
            putMode = true;
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

            long ts = getSafeTimestamp(event);
            DatabaseEntry key = new DatabaseEntry(ByteBuffer.allocate(8).putLong(0, ts).array());
            DatabaseEntry value = new DatabaseEntry(byteOutput.toByteArray());

            // i don't care if the count goes over for a bit

            long startWaitTime = System.currentTimeMillis();
            while (stats.getCounterStat(BerkeleyChannel.STAT_CHANNEL_SIZE) >= maxChannelSize
                    && (System.currentTimeMillis() - startWaitTime) > maxPutWaitTime) {
                Thread.sleep(5);
            }

            if (stats.getCounterStat(BerkeleyChannel.STAT_CHANNEL_SIZE) < maxChannelSize) {
                db.put(txn, key, value);
                stats.incrementCounter(BerkeleyChannel.STAT_CHANNEL_SIZE, 1);
                count++;
            }
            else {
                throw new ChannelException("Channel max of " + maxChannelSize
                        + " has been reached - 'take'ers are not keeping up");
            }
        }
        finally {
            long duration = (System.nanoTime() - statStartTime) / 1000;
            logger.debug("put duration (micros) = {}", duration);
            stats.updateRollingStat(BerkeleyChannel.STAT_PUT, 1, duration);
        }
    }

    @Override
    protected Event doTake() throws InterruptedException {
        if (putMode) {
            throw new ChannelException(
                    "This transaction has already been used for 'put'.  Cannot mix 'put' and 'take' on same transaction");
        }

        long statStartTime = System.nanoTime();

        if (null == cursor) {
            cursor = db.openCursor(txn, null);
        }

        try {
            DatabaseEntry foundKey = new DatabaseEntry();
            DatabaseEntry foundData = new DatabaseEntry();

            while (cursor.getNext(foundKey, foundData, null) == OperationStatus.SUCCESS) {
                ByteArrayInputStream bais = new ByteArrayInputStream(foundData.getData());
                DataInputStream in = new DataInputStream(bais);
                FlumeEvent event = null;
                try {
                    event = FlumeEvent.from(in);
                }
                catch (IOException e) {
                    logger.error("could not deserialize 'Event' object - trashing and moving on", e);
                }
                cursor.delete();
                if (null != event) {
                    stats.incrementCounter(BerkeleyChannel.STAT_CHANNEL_SIZE, -1);
                    count--;
                    return event;
                }
            }
            return null;
        }
        catch (DatabaseException e) {
            throw new ChannelException("Take failed because of database issue", e);
        }
        finally {
            long duration = (System.nanoTime() - statStartTime) / 1000;
            logger.debug("take duration (micros) = {}", duration);
            stats.updateRollingStat(BerkeleyChannel.STAT_TAKE, 1, duration);
        }
    }

    @Override
    protected void doCommit() throws InterruptedException {
        cleanupCursor();
        txn.commit();
        if (0 != count) {
            stats.incrementCounter(BerkeleyChannel.STAT_COMMITS, 1);
        }
    }

    @Override
    protected void doRollback() throws InterruptedException {
        cleanupCursor();
        txn.abort();
        stats.incrementCounter(BerkeleyChannel.STAT_ROLLBACKS, 1);
        if (0 != count) {
            stats.incrementCounter(BerkeleyChannel.STAT_CHANNEL_SIZE, -count);
        }
    }

    private void cleanupCursor() {
        if (null != cursor) {
            cursor.close();
            cursor = null;
        }
    }

    private long getSafeTimestamp(Event event) {
        Map<String, String> headerMap = event.getHeaders();
        String tmp = null != headerMap ? headerMap.get(timestampHeader) : null;
        long ts = null != tmp ? Long.valueOf(tmp) : System.currentTimeMillis() * 1000; // in microseconds
        return ts;
    }
}
