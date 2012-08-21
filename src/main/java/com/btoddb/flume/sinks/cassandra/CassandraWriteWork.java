package com.btoddb.flume.sinks.cassandra;

import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import me.prettyprint.cassandra.serializers.ByteBufferSerializer;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;

public class CassandraWriteWork implements Runnable {
    private static Logger logger = LoggerFactory.getLogger(CassandraWriteWork.class);
    
    Mutator<ByteBuffer> mutator;
    private CassandraWorkStatus callback;

    public CassandraWriteWork(Keyspace keyspace, CassandraWorkStatus callback) {
        this.callback = callback;

        mutator = HFactory.createMutator(keyspace, ByteBufferSerializer.get());
    }

    public void run() {
        try {
            mutator.execute();
        }
        catch (Throwable e) {
            logger.error("exception while executing mutator", e);
        }
        callback.finished(this);
    }

    public Mutator<ByteBuffer> getMutator() {
        return mutator;
    }
}
