/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flume.sink.cassandra;

import java.lang.management.ManagementFactory;
import java.util.LinkedList;
import java.util.List;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import me.prettyprint.hector.api.exceptions.HectorException;

import org.apache.flume.Channel;
import org.apache.flume.ChannelException;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.instrumentation.SinkCounter;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CassandraSink extends AbstractSink implements Configurable, CassandraSinkMBean {
    private static final Logger logger = LoggerFactory.getLogger(CassandraSink.class);

    private static final String MBEAN_NAME_ROOT = "org.apache.flume.sink:type=";
    private static final int MAX_BATCH_SIZE = 100;
    private static final String STAT_SAVE = "cass-save";
    private static final String STAT_TAKE = "channel-take";
    private static final String STAT_BATCH_SIZE = "batch-size";

    private int maxSaveBatchSize = MAX_BATCH_SIZE;

    private CassandraSinkRepository repository;
    private MBeanServer mbs;
    // private int lastSaveBatchSize;
    // private long lastSaveDurationInMicros;

    private SinkCounter sinkCounter;
    private JmxStatsHelper stats;

    // private long windowStartTime;
    // private long windowDuration = 60 * 1000;
    // private long runningSaveDurationInMicros;
    // private int runningSaveCount;
    //
    // private long runningChannelTakeInMicros;

    @Override
    public void configure(Context context) {
        String hosts = context.getString("hosts");
        int port = context.getInteger("port", 9160);
        String clusterName = context.getString("cluster-name");
        String keyspaceName = context.getString("keyspace-name");
        String hoursColFamName = context.getString("hours-colfam", "hours");
        String recordsColFamName = context.getString("records-colfam", "records");
        int socketTimeoutMillis = context.getInteger("socket-timeout-millis", 5000);
        int maxConnectionsPerHost = context.getInteger("max-conns-per-host", 2);
        int maxExhaustedWaitMillis = context.getInteger("max-exhausted-wait-millis", 5000);

        repository = new CassandraSinkRepository();

        repository.setHosts(hosts);
        repository.setPort(port);
        repository.setClusterName(clusterName);
        repository.setKeyspaceName(keyspaceName);
        repository.setHoursColFamName(hoursColFamName);
        repository.setRecordsColFamName(recordsColFamName);
        repository.setSocketTimeoutMillis(socketTimeoutMillis);
        repository.setMaxConnectionsPerHost(maxConnectionsPerHost);
        repository.setMaxExhaustedWaitMillis(maxExhaustedWaitMillis);

        if (sinkCounter == null) {
            sinkCounter = new SinkCounter(getName());
        }

        if (null == stats) {
            stats = new JmxStatsHelper(60 * 1000);
        }

        mbs = ManagementFactory.getPlatformMBeanServer();
        try {
            mbs.registerMBean(this, new ObjectName(MBEAN_NAME_ROOT + getName() + "-perf"));
        }
        catch (Throwable e) {
            logger.error("exception while registering me as mbean, " + MBEAN_NAME_ROOT + getName(), e);
        }
    }

    @Override
    public void start() {
        repository.initHector();
        stats.resetStatsWindow();
        sinkCounter.start();
        super.start();
    }

    @Override
    public void stop() {
        super.stop();
        sinkCounter.stop();
    }

    @Override
    public Status process() throws EventDeliveryException {
        logger.debug("start processing");

        Status status = Status.BACKOFF;
        Channel channel = getChannel();
        Transaction transaction = channel.getTransaction();
        List<Event> eventList = new LinkedList<Event>();

        try {
            transaction.begin();

            // gather a batch of saves
            for (int i = 0; i < maxSaveBatchSize; i++) {
                long channelStartTime = System.nanoTime();
                Event event;
                try {
                    event = channel.take();
                }
                finally {
                    long duration = (System.nanoTime() - channelStartTime) / 1000;
                    logger.debug( "take duration (micros) = {}", duration);
                    stats.update(STAT_TAKE, 1, duration);
                }
                sinkCounter.incrementEventDrainAttemptCount();

                if (null != event) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("event: " + event.toString());
                    }
                    eventList.add(event);
                    status = Status.READY;
                }
                else {
                    break;
                }
            }

            if (!eventList.isEmpty()) {
                stats.update(STAT_BATCH_SIZE, 1, eventList.size());
                if (eventList.size() == maxSaveBatchSize) {
                    sinkCounter.incrementBatchCompleteCount();
                }
                else {
                    sinkCounter.incrementBatchUnderflowCount();
                }

                long start = System.nanoTime();
                try {
                    repository.saveToCassandra(eventList);
                }
                finally {
                    stats.update(STAT_SAVE, eventList.size(), (System.nanoTime() - start) / 1000);
                }

                sinkCounter.addToEventDrainSuccessCount(eventList.size());
            }
            else {
                sinkCounter.incrementBatchEmptyCount();
            }

            transaction.commit();
        }
        catch (ChannelException e) {
            transaction.rollback();
            status = Status.BACKOFF;
            sinkCounter.incrementConnectionFailedCount();
        }
        catch (HectorException e) {
            sinkCounter.incrementConnectionFailedCount();
            transaction.rollback();
            logger.error("exception while persisting to cassandra", e);
            throw new EventDeliveryException("Failed to persist message to cassandra", e);
        }
        catch (Throwable e) {
            transaction.rollback();
            logger.error("exception while processing in Cassandra Sink", e);
            throw new EventDeliveryException("Failed to persist message", e);
        }
        finally {
            transaction.close();
        }

        return status;
    }

    public CassandraSinkRepository getRepository() {
        return repository;
    }

    public void setRepository(CassandraSinkRepository repository) {
        this.repository = repository;
    }

    // @Override
    // public int getLastSaveBatchSize() {
    // return lastSaveBatchSize;
    // }
    //
    // @Override
    // public long getLastSaveDurationInMicros() {
    // return lastSaveDurationInMicros;
    // }

    @Override
    public int getMaxSaveBatchSize() {
        return maxSaveBatchSize;
    }

    @Override
    public void setMaxSaveBatchSize(int maxBatchSize) {
        this.maxSaveBatchSize = maxBatchSize;
    }

    @Override
    public int getSaveAvgInMicros() {
        return stats.getStat(STAT_SAVE).getAverageAmount();
    }

    @Override
    public int getSavesPerSecond() {
        return stats.getStat(STAT_SAVE).getCountPerSecond();
    }

    @Override
    public int getTakeAvgInMicros() {
        return stats.getStat(STAT_TAKE).getAverageAmount();
    }

    @Override
    public int getTakesPerSecond() {
        return stats.getStat(STAT_TAKE).getCountPerSecond();
    }

    @Override
    public int getBatchSizeAvg() {
        return stats.getStat(STAT_BATCH_SIZE).getAverageAmount();
    }

}
