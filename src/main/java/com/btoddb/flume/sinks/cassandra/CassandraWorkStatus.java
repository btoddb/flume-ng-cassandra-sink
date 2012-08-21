package com.btoddb.flume.sinks.cassandra;

public interface CassandraWorkStatus {

    void finished(CassandraWriteWork work);
    
}
