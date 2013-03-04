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
    private boolean success = false;
    private CassandraJobWorkException exception;

    public CassandraWriteWork(Keyspace keyspace, CassandraWorkStatus callback) {
        this.callback = callback;

        mutator = HFactory.createMutator(keyspace, ByteBufferSerializer.get());
    }

    public void run() {
        try {
            mutator.execute();
            success = true;
        }
        catch (Throwable e) {
            logger.error("exception while executing mutator", e);
            exception = new CassandraJobWorkException("uncaught exeception while executing mutation", e);
            success = false;
        }
        callback.finished(this);
    }

    public Mutator<ByteBuffer> getMutator() {
        return mutator;
    }

    public CassandraJobWorkException getException() {
        return exception;
    }

    public boolean isSuccess() {
        return success;
    }

}
