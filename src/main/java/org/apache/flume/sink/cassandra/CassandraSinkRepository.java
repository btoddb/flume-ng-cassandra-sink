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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import me.prettyprint.cassandra.connection.SpeedForJOpTimer;
import me.prettyprint.cassandra.serializers.ByteBufferSerializer;
import me.prettyprint.cassandra.serializers.BytesArraySerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.serializers.UUIDSerializer;
import me.prettyprint.cassandra.service.CassandraHostConfigurator;
import me.prettyprint.cassandra.service.OperationType;
import me.prettyprint.cassandra.utils.TimeUUIDUtils;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.ConsistencyLevelPolicy;
import me.prettyprint.hector.api.HConsistencyLevel;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.Row;
import me.prettyprint.hector.api.beans.Rows;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.MultigetSliceQuery;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.SliceQuery;

import org.apache.flume.Event;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeFormatterBuilder;
import org.joda.time.format.ISODateTimeFormat;

public class CassandraSinkRepository {
	private static DateTimeFormatter dfEvent = ISODateTimeFormat.dateTime()
			.withOffsetParsed();
	private static DateTimeFormatter dfHourKey = new DateTimeFormatterBuilder()
			.appendYear(4, 4).appendMonthOfYear(2).appendDayOfMonth(2)
			.appendHourOfDay(2).toFormatter();

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
			} else if (OperationType.WRITE == op) {
				return HConsistencyLevel.ONE;
			} else {
				return HConsistencyLevel.QUORUM;
			}
		}
	};

	private static final Object EMPTY_BYTE_ARRAY = new byte[0];

	private String hosts;
	private int port;
	private String clusterName;
	private String keyspaceName;
	private int socketTimeoutMillis;
	private int maxConnectionsPerHost;
	private int maxExhaustedWaitMillis;
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
		keyspace = HFactory.createKeyspace(keyspaceName, cluster,
				CONSISTENCY_LEVEL_POLICY);
	}

	public void saveToCassandra(List<Event> eventList) {
		Mutator<ByteBuffer> m = HFactory.createMutator(keyspace,
				ByteBufferSerializer.get());
		for (Event event : eventList) {
			Map<String, String> headers = event.getHeaders();

			DateTime ts = getTime(headers.get("timestamp"));
			String src = null != headers.get("src") ? headers.get("src") : "not-set";
			String host = headers.get("host");

			UUID tsUuid = createTimeUUID(ts);
			String hour = getHourFromTime(ts);

			ByteBuffer recordsKey = TimeUUIDUtils.asByteBuffer(tsUuid);
			m.addInsertion(recordsKey, recordsColFamName,
					HFactory.createColumn("ts", ts.getMillis()));
			m.addInsertion(recordsKey, recordsColFamName,
					HFactory.createColumn("src", src));
			m.addInsertion(recordsKey, recordsColFamName,
					HFactory.createColumn("host", host));
			m.addInsertion(recordsKey, recordsColFamName,
					HFactory.createColumn("data", event.getBody()));

			m.addInsertion(ByteBuffer.wrap(hour.getBytes()), hoursColFamName,
					HFactory.createColumn(tsUuid, EMPTY_BYTE_ARRAY));
		}
		m.execute();
	}

	private String getHourFromTime(DateTime ts) {
		return dfHourKey.print(ts.withZone(DateTimeZone.UTC));
	}

	private DateTime getTime(String tsAsStr) {
		if (null != tsAsStr && !tsAsStr.isEmpty()) {
//			return dfEvent.parseDateTime(tsAsStr);
			return new DateTime(Long.parseLong(tsAsStr));
		} else {
			return new DateTime();
		}

	}

	private UUID createTimeUUID(DateTime ts) {
		return TimeUUIDUtils.getTimeUUID(ts.getMillis());
	}

	public List<LogEvent> getEventsForHour(String hour) {
		SliceQuery<String, UUID, byte[]> dayQuery = HFactory.createSliceQuery(
				keyspace, StringSerializer.get(), UUIDSerializer.get(),
				BytesArraySerializer.get());
		dayQuery.setColumnFamily(hoursColFamName);
		dayQuery.setKey(hour);
		dayQuery.setRange(null, null, false, 100);

		// TODO:BTB - this will need to be paginated
		QueryResult<ColumnSlice<UUID, byte[]>> dayResult = dayQuery.execute();

		ColumnSlice<UUID, byte[]> daySlice = null != dayResult ? dayResult
				.get() : null;
		List<HColumn<UUID, byte[]>> dayColList = null != daySlice ? daySlice
				.getColumns() : null;
		if (null != dayColList && !dayColList.isEmpty()) {
			List<UUID> recordKeyList = new ArrayList<UUID>();
			for (HColumn<UUID, byte[]> col : dayColList) {
				recordKeyList.add(col.getName());
			}

			// TODO:BTB - this will need to be done in pages
			return getRecords(recordKeyList);
		} else {
			return Collections.emptyList();
		}
	}

	private List<LogEvent> getRecords(List<UUID> recordKeyList) {
		MultigetSliceQuery<UUID, String, byte[]> q = HFactory
				.createMultigetSliceQuery(keyspace, UUIDSerializer.get(),
						StringSerializer.get(), BytesArraySerializer.get());
		q.setColumnFamily(recordsColFamName);
		q.setKeys(recordKeyList);
		q.setRange(null, null, false, 100);

		// get the records
		// TODO:BTB - this will need to be paginated
		QueryResult<Rows<UUID, String, byte[]>> result = q.execute();

		// make sure we got something and dig for the data
		Rows<UUID, String, byte[]> rows = null != result ? result.get() : null;
		Iterator<Row<UUID, String, byte[]>> iter = null != rows ? rows
				.iterator() : null;
		if (null == iter) {
			return Collections.emptyList();
		}

		// iterate over the rows returned
		List<LogEvent> logEventList = new LinkedList<LogEvent>();
		while (iter.hasNext()) {
			Row<UUID, String, byte[]> row = iter.next();
			ColumnSlice<String, byte[]> slice = null != row ? row
					.getColumnSlice() : null;
			List<HColumn<String, byte[]>> colList = null != slice ? slice
					.getColumns() : null;

			// it is possible to get a row with no columns (has tombstones)
			if (null != colList && !colList.isEmpty()) {
				HColumn<String, byte[]> tmp = slice.getColumnByName("src");
				String src = new String(tmp.getValue());
				String host = new String(slice.getColumnByName("host")
						.getValue());
				byte[] data = slice.getColumnByName("data").getValue();

				LogEvent logEvt = new LogEvent(
						TimeUUIDUtils.getTimeFromUUID(row.getKey()), data);
				logEventList.add(logEvt);
			}
		}

		return logEventList;
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

}
