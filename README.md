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
    ;


Sample Flume config
-------------

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

    agent.sinks.cassandraSink.type = com.btoddb.flume.sinks.cassandra.CassandraSink
    agent.sinks.cassandraSink.hosts = localhost
    agent.sinks.cassandraSink.cluster-name = Logging
    agent.sinks.cassandraSink.keyspace-name = logs
    agent.sinks.cassandraSink.max-conns-per-host = 2

Building Cassandra Sink
-----------------------

The sink is built using Maven

   mvn clean package -P assemble-artifacts

... runs all junits and produces flume-ng-cassandra-sink-1.0.0-SNAPSHOT.jar and
    flume-ng-cassandra-sink-1.0.0-SNAPSHOT-dist.tar.gz

The tar contains all the dependencies needed, and then some.  See the list below regarding what is actually needed
to use the sink in the flume environment.

Required Dependencies
---------------------

* flume-ng-cassandra-sink*.jar
* hector-core*
* guava*
* speed4j*
* uuid*
* libthrift*
* cassandra-thrift*

