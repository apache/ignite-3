/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.raft.jraft.counter;

import java.nio.ByteBuffer;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import org.apache.ignite.raft.jraft.Status;
import org.apache.ignite.raft.jraft.closure.ReadIndexClosure;
import org.apache.ignite.raft.jraft.entity.Task;
import org.apache.ignite.raft.jraft.error.RaftError;
import org.apache.ignite.raft.jraft.util.BytesUtil;
import org.apache.ignite.raft.jraft.util.Marshaller;
import org.apache.ignite.raft.jraft.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class CounterServiceImpl implements CounterService {
    private static final Logger LOG = LoggerFactory.getLogger(CounterServiceImpl.class);

    private final CounterServer counterServer;
    private final Executor readIndexExecutor;

    public CounterServiceImpl(CounterServer counterServer) {
        this.counterServer = counterServer;
        this.readIndexExecutor = createReadIndexExecutor();
    }

    private Executor createReadIndexExecutor() {
        return Executors.newCachedThreadPool();
    }

    @Override
    public void get(final boolean readOnlySafe, final CounterClosure closure) {
        if (!readOnlySafe) {
            closure.success(getValue());
            closure.run(Status.OK());
            return;
        }

        this.counterServer.getNode().readIndex(BytesUtil.EMPTY_BYTES, new ReadIndexClosure() {
            @Override
            public void run(Status status, long index, byte[] reqCtx) {
                if (status.isOk()) {
                    closure.success(getValue());
                    closure.run(Status.OK());
                    return;
                }
                CounterServiceImpl.this.readIndexExecutor.execute(() -> {
                    if (isLeader()) {
                        LOG.debug("Fail to get value with 'ReadIndex': {}, try to applying to the state machine.", status);
                        applyOperation(CounterOperation.createGet(), closure);
                    }
                    else {
                        handlerNotLeaderError(closure);
                    }
                });
            }
        });
    }

    private boolean isLeader() {
        return this.counterServer.getFsm().isLeader();
    }

    private long getValue() {
        return this.counterServer.getFsm().getValue();
    }

    private String getRedirect() {
        return this.counterServer.redirect().getRedirect();
    }

    @Override
    public void incrementAndGet(final long delta, final CounterClosure closure) {
        applyOperation(CounterOperation.createIncrement(delta), closure);
    }

    private void applyOperation(final CounterOperation op, final CounterClosure closure) {
        if (!isLeader()) {
            handlerNotLeaderError(closure);
            return;
        }

        try {
            closure.setCounterOperation(op);
            final Task task = new Task();
            task.setData(ByteBuffer.wrap(Marshaller.DEFAULT.marshall(op)));
            task.setDone(closure);
            this.counterServer.getNode().apply(task);
        }
        catch (Exception e) {
            String errorMsg = "Fail to encode CounterOperation";
            LOG.error(errorMsg, e);
            closure.failure(errorMsg, StringUtils.EMPTY);
            closure.run(new Status(RaftError.EINTERNAL, errorMsg));
        }
    }

    private void handlerNotLeaderError(final CounterClosure closure) {
        closure.failure("Not leader.", getRedirect());
        closure.run(new Status(RaftError.EPERM, "Not leader"));
    }
}
