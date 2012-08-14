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

package com.btoddb.flume.sinks.cassandra;

import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import me.prettyprint.cassandra.connection.SpeedForJOpTimer;
import me.prettyprint.cassandra.serializers.ByteBufferSerializer;
import me.prettyprint.cassandra.service.CassandraHostConfigurator;
import me.prettyprint.cassandra.service.OperationType;
import me.prettyprint.cassandra.utils.TimeUUIDUtils;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.ConsistencyLevelPolicy;
import me.prettyprint.hector.api.HConsistencyLevel;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;

import org.apache.flume.Event;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeFormatterBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CassandraSinkRepository {
    private static final Logger logger = LoggerFactory.getLogger(CassandraSinkRepository.class);

    // all reads/writes for cached data are ONE, meta reads/writes are at quorum
    private static final ConsistencyLevelPolicy CONSISTENCY_LEVEL_POLICY = new ConsistencyLevelPolicy() {
        @Override
        public HConsistencyLevel get(OperationType op, String cfName) {
            return get(op);
        }

        @Override
        public HConsistencyLevel get(OperationType op) {
            if (OperationType.READ == op) {
                return HConsistencyLevel.ONE;
            }
            else if (OperationType.WRITE == op) {
                return HConsistencyLevel.ONE;
            }
            else {
                return HConsistencyLevel.QUORUM;
            }
        }
    };

    private static DateTimeFormatter dfHourKey = new DateTimeFormatterBuilder().appendYear(4, 4).appendMonthOfYear(2)
            .appendDayOfMonth(2).appendHourOfDay(2).toFormatter();

    private static final TimeUnit TIMEUNIT_DEFAULT = TimeUnit.MICROSECONDS;
    private static final Object EMPTY_BYTE_ARRAY = new byte[0];

    private TimeUnit timeUnit = TIMEUNIT_DEFAULT;
    private String hosts;
    private int port;
    private String clusterName;
    private String keyspaceName;
    private int socketTimeoutMillis;
    private int maxConnectionsPerHost;
    private int maxExhaustedWaitMillis;
    private int maxColumnBatchSize = 100;

    // used to distribute load around the cluster
    private byte scatterValue;
    private byte nextScatter = 0;
    private LogEventColumnTranslator columnTranslator;

    private String hoursColFamName = "hours";
    private String recordsColFamName = "records";

    private Cluster cluster;
    private Keyspace keyspace;

    public void initHector() {
        // TODO:btb - go thru these later to be sure they are set correct
        // and properly parameterized by config file
        CassandraHostConfigurator cassConfig = new CassandraHostConfigurator();
        cassConfig.setAutoDiscoverHosts(true);
        cassConfig.setAutoDiscoveryDelayInSeconds(600); // 5 minutes
        cassConfig.setCassandraThriftSocketTimeout(socketTimeoutMillis);
        cassConfig.setHosts(hosts);
        cassConfig.setMaxActive(maxConnectionsPerHost);
        cassConfig.setMaxWaitTimeWhenExhausted(maxExhaustedWaitMillis);
        cassConfig.setOpTimer(new SpeedForJOpTimer(clusterName));
        cassConfig.setPort(port);
        cassConfig.setRunAutoDiscoveryAtStartup(true);
        cassConfig.setUseThriftFramedTransport(true);
        cassConfig.setRetryDownedHosts(true);
        cassConfig.setRetryDownedHostsDelayInSeconds(10);
        cassConfig.setRetryDownedHostsQueueSize(-1); // no bounds

        cluster = HFactory.createCluster(clusterName, cassConfig, null);
        keyspace = HFactory.createKeyspace(keyspaceName, cluster, CONSISTENCY_LEVEL_POLICY);
        
        columnTranslator = new LogEventColumnTranslator(keyspace, recordsColFamName);
    }

    public void saveToCassandra(List<Event> eventList) {
        Mutator<ByteBuffer> m = HFactory.createMutator(keyspace, ByteBufferSerializer.get());
        for (Event event : eventList) {
            FlumeLogEvent flumeLog = new FlumeLogEvent(event, timeUnit);

            long ts = flumeLog.getTimestamp();
            String src = flumeLog.getSource();
            String host = flumeLog.getHost();

            UUID tsUuid = createTimeUUIDFromTimestamp(ts);

            ByteBuffer recordsKey = TimeUUIDUtils.asByteBuffer(tsUuid);
            m.addInsertion(recordsKey, recordsColFamName, HFactory.createColumn("ts", ts));
            m.addInsertion(recordsKey, recordsColFamName, HFactory.createColumn("src", src));
            m.addInsertion(recordsKey, recordsColFamName, HFactory.createColumn("host", host));
            m.addInsertion(recordsKey, recordsColFamName, HFactory.createColumn("data", event.getBody()));

            m.addInsertion(createTimeBasedKey(ts), hoursColFamName, HFactory.createColumn(tsUuid, EMPTY_BYTE_ARRAY));
        }
        m.execute();
    }

    // time based keys use a "scatter value" to scatter the writes across the cassandra cluster distributing the load
    // instead of burning a hole in a single node, making better use of the cassandra cluster
    private ByteBuffer createTimeBasedKey(String hour, byte scatterValue) {
        ByteBuffer bb = ByteBuffer.allocate(hour.length() + 3);
        bb.put(hour.getBytes()).put(String.format("%03d", scatterValue).getBytes());
        bb.rewind();
        return bb;
    }

    private ByteBuffer createTimeBasedKey(long timestamp) {
        String hour = getHourFromTimestamp(timestamp);
        ByteBuffer bb = createTimeBasedKey(hour, (byte)nextScatter);
        nextScatter = (byte)((nextScatter + 1) % scatterValue);
        return bb;
    }

    public String getHourFromTimestamp(long ts) {
        return dfHourKey.print(new DateTime(timeUnit.toMillis(ts)).withZone(DateTimeZone.UTC));
    }

    private UUID createTimeUUIDFromTimestamp(long ts) {
        return TimeUUIDUtils.getTimeUUID(ts);
    }

    public Iterator<LogEvent> getEventsForHour(String hour) {
        ByteBuffer[] keyArr = new ByteBuffer[scatterValue];
        for (int i = 0; i < scatterValue; i++) {
            keyArr[i] = createTimeBasedKey(hour, (byte) i);
        }
        MultiRowMergeColumnIterator iter = new MultiRowMergeColumnIterator(keyspace, hoursColFamName, keyArr,
                TimeUUIDComparator.INSTANCE, columnTranslator, maxColumnBatchSize);
        return iter;
    }

    public String getHosts() {
        return hosts;
    }

    public void setHosts(String hosts) {
        this.hosts = hosts;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getClusterName() {
        return clusterName;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    public String getKeyspaceName() {
        return keyspaceName;
    }

    public void setKeyspaceName(String keyspaceName) {
        this.keyspaceName = keyspaceName;
    }

    public int getSocketTimeoutMillis() {
        return socketTimeoutMillis;
    }

    public void setSocketTimeoutMillis(int socketTimeoutMillis) {
        this.socketTimeoutMillis = socketTimeoutMillis;
    }

    public int getMaxConnectionsPerHost() {
        return maxConnectionsPerHost;
    }

    public void setMaxConnectionsPerHost(int maxConnectionsPerHost) {
        this.maxConnectionsPerHost = maxConnectionsPerHost;
    }

    public int getMaxExhaustedWaitMillis() {
        return maxExhaustedWaitMillis;
    }

    public void setMaxExhaustedWaitMillis(int maxExhaustedWaitMillis) {
        this.maxExhaustedWaitMillis = maxExhaustedWaitMillis;
    }

    public String getHoursColFamName() {
        return hoursColFamName;
    }

    public void setHoursColFamName(String hoursColFamName) {
        this.hoursColFamName = hoursColFamName;
    }

    public String getRecordsColFamName() {
        return recordsColFamName;
    }

    public void setRecordsColFamName(String recordsColFameName) {
        this.recordsColFamName = recordsColFameName;
    }

    public void setTimeUnit(TimeUnit timeUnit) {
        this.timeUnit = timeUnit;
    }

    public void setScatterValue(byte scatterValue) {
        this.scatterValue = scatterValue;
    }

    public byte getScatterValue() {
        return scatterValue;
    }

    // -----------

    public static class TimeUUIDComparator implements Comparator<UUID> {
        public static final TimeUUIDComparator INSTANCE = new TimeUUIDComparator();

        @Override
        public int compare(UUID o1, UUID o2) {
            if (null == o1 && null == o2) {
                return 0;
            }
            else if (null == o1) {
                return -1;
            }
            else if (null == o2) {
                return 1;
            }
            else {
                long t1 = TimeUUIDUtils.getTimeFromUUID(o1);
                long t2 = TimeUUIDUtils.getTimeFromUUID(o2);
                return t1 < t2 ? -1 : (t1 > t2 ? 1 : o1.compareTo(o2));
            }
        }
    }

    public int getMaxColumnBatchSize() {
        return maxColumnBatchSize;
    }

    public void setMaxColumnBatchSize(int maxColumnBatchSize) {
        this.maxColumnBatchSize = maxColumnBatchSize;
    }

}
