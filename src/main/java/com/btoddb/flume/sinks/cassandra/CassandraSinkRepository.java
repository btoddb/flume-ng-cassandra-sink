/*
 * Copyright [2013] B. Todd Burruss
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import me.prettyprint.cassandra.connection.SpeedForJOpTimer;
import me.prettyprint.cassandra.service.CassandraHostConfigurator;
import me.prettyprint.cassandra.service.OperationType;
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
import org.joda.time.format.ISODateTimeFormat;

public class CassandraSinkRepository {
    // private static final Logger logger = LoggerFactory.getLogger(CassandraSinkRepository.class);

    // all reads/writes for data are ONE, meta reads/writes are at quorum
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

    private static DateTimeFormatter DF_ISO = ISODateTimeFormat.dateTime();

    private static final int MAX_QUEUED_WORK = 1000;

    private String hosts;
    private int port;
    private String clusterName;
    private String keyspaceName;
    private int socketTimeoutMillis;
    private int maxConnectionsPerHost;
    private int maxExhaustedWaitMillis;

    private String recordsColFamName = "records";

    private Cluster cluster;
    private Keyspace keyspace;

    private int numWorkThreads = 8;
    private ExecutorService workExecutor;

    public void init() {
        workExecutor = new ThreadPoolExecutor(numWorkThreads, numWorkThreads, 20, TimeUnit.MINUTES,
                new ArrayBlockingQueue<Runnable>(MAX_QUEUED_WORK));

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

    }

    public void saveToCassandra(List<Event> eventList) throws InterruptedException, ExecutionException{
        CassandraJob job = new CassandraJob(keyspace, workExecutor);
        for (Event event : eventList) {
            FlumeLogEvent flumeLog = new FlumeLogEvent(event);

            long tsInMillis = flumeLog.getTimestamp();
            String src = flumeLog.getSource();
            String host = flumeLog.getHost();
            String key = flumeLog.getKey();

            if (null == key) {
                throw new IllegalArgumentException("Missing flume header attribute, 'key' - cannot process this event");
            }

            String tsAsStr = createTimeStampAsString(tsInMillis);

            ByteBuffer recordsKey = ByteBuffer.wrap(new String(src + ":" + key).getBytes());

            job.beginWorkUnit();
            Mutator<ByteBuffer> m = job.getMutator();
            m.addInsertion(recordsKey, recordsColFamName, HFactory.createColumn("ts", tsAsStr));
            m.addInsertion(recordsKey, recordsColFamName, HFactory.createColumn("src", src));
            m.addInsertion(recordsKey, recordsColFamName, HFactory.createColumn("host", host));
            m.addInsertion(recordsKey, recordsColFamName, HFactory.createColumn("data", event.getBody()));
            job.submitWorkUnit();
        }

        job.allWorkSubmitted();
        job.waitUntilFinished();
    }

    private String createTimeStampAsString(long ts) {
        return DF_ISO.print(new DateTime(ts).withZone(DateTimeZone.UTC));
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

    public String getRecordsColFamName() {
        return recordsColFamName;
    }

    public void setRecordsColFamName(String recordsColFameName) {
        this.recordsColFamName = recordsColFameName;
    }

    public Keyspace getKeyspace() {
        return keyspace;
    }

    public Cluster getCluster() {
        return cluster;
    }

}
