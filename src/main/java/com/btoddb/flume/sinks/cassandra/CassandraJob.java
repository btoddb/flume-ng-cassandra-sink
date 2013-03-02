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
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.mutation.Mutator;

/**
 * Not thread-safe and therefore cannot be used by multiple threads without synchronization.
 * 
 * @author bburruss
 * 
 */
public class CassandraJob implements CassandraWorkStatus {
    private ExecutorService workExec;
    private Set<CassandraWriteWork> workSet = new HashSet<CassandraWriteWork>();
    private int maxUnitsPerCommit = 100;
    private int count = 0;
    private CassandraWriteWork work;
    private Keyspace keyspace;

    public CassandraJob(Keyspace keyspace, ExecutorService workExec) {
        this.keyspace = keyspace;
        this.workExec = workExec;
    }

    @Override
    public void finished(CassandraWriteWork work) {
        synchronized (workSet) {
            if (!workSet.remove(work)) {
                throw new IllegalStateException("Could not find 'work' in write work set.  This should never happen!!");
            }

            if (workSet.isEmpty()) {
                workSet.notifyAll();
            }
        }
    }

    public void waitUntilFinished() {
        synchronized (workSet) {
            while (!workSet.isEmpty()) {
                try {
                    workSet.wait();
                }
                catch (InterruptedException e) {
                    Thread.interrupted();
                }
            }
        }
    }

    public void allWorkSubmitted() throws InterruptedException, ExecutionException{
        submitWork();
    }

    public void beginWorkUnit() {
        if (null == work) {
            work = new CassandraWriteWork(keyspace, this);
        }
        // else {
        // throw new
        // IllegalStateException("previous work unit not submitted.  must call 'submitWorkUnit' before beginning a new one");
        // }
    }

    public void submitWorkUnit() throws InterruptedException, ExecutionException{
        if (0 == ++count % maxUnitsPerCommit) {
            submitWork();
        }
    }

    private void submitWork() throws InterruptedException, ExecutionException{
        if (null != work) {
            workSet.add(work);
            Future<?> future = workExec.submit(work);
            work = null;
            //future's get method will help in propagating the exception if the 
            // task has failed.
            future.get();
        }
    }

    public Mutator<ByteBuffer> getMutator() {
        if (null != work) {
            return work.getMutator();
        }
        else {
            throw new IllegalStateException(
                    "you must begin a work unit before getting mutator.  call 'beginWorkUnit' before getting mutator");
        }
    }
}
