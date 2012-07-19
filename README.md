flume-cassandra-sink
====================

A Flume sink using Apache Cassandra

Cassandra Schema
----------------

    create keyspace logs with
       strategy_options = {datacenter1:1}
    ;

    use logs;

    create column family records with
       comparator = UTF8Type
       and gc_grace = 86400
       and column_metadata = [
         {column_name: ts, validation_class: LongType, index_type: KEYS}
         {column_name: src, validation_class: UTF8Type, index_type: KEYS}
         {column_name: host, validation_class: UTF8Type, index_type: KEYS}
       ]
    ;

    create column family hours with
       comparator = TimeUUIDType
       and gc_grace = 86400
    ;


Sample Flume config
-------------

`
agent.sources = avrosource
agent.channels = channel1
agent.sinks = cassandraSink

agent.sources.avrosource.type = avro
agent.sources.avrosource.channels = channel1
agent.sources.avrosource.bind = 0.0.0.0
agent.sources.avrosource.port = 4141

agent.sources.avrosource.interceptors = addHost addTimestamp
agent.sources.avrosource.interceptors.addHost.type = org.apache.flume.interceptor.HostInterceptor$Builder
agent.sources.avrosource.interceptors.addHost.preserveExisting = false
agent.sources.avrosource.interceptors.addHost.useIP = false
agent.sources.avrosource.interceptors.addHost.hostHeader = host

agent.sources.avrosource.interceptors.addTimestamp.type = org.apache.flume.interceptor.TimestampInterceptor$Builder

# Cassandra flow
agent.channels.channel1.type = FILE
agent.channels.channel1.checkpointDir = file-channel1/check
agent.channels.channel1.dataDirs = file-channel1/data

agent.sinks.cassandraSink.channel = channel1

agent.sinks.cassandraSink.type = org.apache.flume.sink.cassandra.CassandraSink
agent.sinks.cassandraSink.hosts = localhost
agent.sinks.cassandraSink.cluster-name = Logging
agent.sinks.cassandraSink.keyspace-name = logs
agent.sinks.cassandraSink.max-conns-per-host = 2
