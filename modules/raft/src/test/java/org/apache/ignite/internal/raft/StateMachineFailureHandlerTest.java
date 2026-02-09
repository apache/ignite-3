/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.raft;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.file.Path;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import org.apache.ignite.internal.failure.FailureContext;
import org.apache.ignite.internal.failure.FailureManager;
import org.apache.ignite.internal.failure.FailureType;
import org.apache.ignite.internal.failure.handlers.FailureHandler;
import org.apache.ignite.internal.raft.server.impl.JraftServerImpl;
import org.apache.ignite.internal.raft.server.impl.JraftServerImpl.DelegatingStateMachine;
import org.apache.ignite.internal.raft.service.CommandClosure;
import org.apache.ignite.internal.raft.service.RaftGroupListener;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.testframework.failure.FailureManagerExtension;
import org.apache.ignite.internal.testframework.failure.MuteFailureManagerLogging;
import org.apache.ignite.raft.jraft.Closure;
import org.apache.ignite.raft.jraft.option.NodeOptions;
import org.apache.ignite.raft.jraft.storage.snapshot.SnapshotReader;
import org.apache.ignite.raft.jraft.storage.snapshot.SnapshotWriter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Test that checks that {@link FailureManager} handles exceptions from {@link RaftGroupListener} correctly.
 */
@ExtendWith(FailureManagerExtension.class)
@MuteFailureManagerLogging // Failures are expected.
public class StateMachineFailureHandlerTest extends BaseIgniteAbstractTest {
    private static final RuntimeException EXPECTED_ERROR = new RuntimeException();

    private static final RaftGroupListener TEST_LISTENER = new RaftGroupListener() {
        @Override
        public void onRead(Iterator<CommandClosure<ReadCommand>> iterator) {

        }

        @Override
        public void onWrite(Iterator<CommandClosure<WriteCommand>> iterator) {
            throw EXPECTED_ERROR;
        }

        @Override
        public void onSnapshotSave(Path path, Consumer<Throwable> doneClo) {
            throw EXPECTED_ERROR;
        }

        @Override
        public boolean onSnapshotLoad(Path path) {
            throw EXPECTED_ERROR;
        }

        @Override
        public void onShutdown() {

        }
    };

    @Test
    public void testOnWrite() {
        AtomicBoolean reached = new AtomicBoolean();

        DelegatingStateMachine sm = new JraftServerImpl.DelegatingStateMachine(
                nodeId(),
                TEST_LISTENER,
                mock(NodeOptions.class),
                testFailureManager(reached)
        );

        sm.onApply(mock(org.apache.ignite.raft.jraft.Iterator.class));

        assertTrue(reached.get());
    }

    @Test
    public void testOnSnapshotSave() {
        AtomicBoolean reached = new AtomicBoolean();

        DelegatingStateMachine sm = new JraftServerImpl.DelegatingStateMachine(
                nodeId(),
                TEST_LISTENER,
                mock(NodeOptions.class),
                testFailureManager(reached)
        );

        SnapshotWriter writer = mock(SnapshotWriter.class);

        when(writer.getPath()).thenReturn("");

        sm.onSnapshotSave(writer, mock(Closure.class));

        assertTrue(reached.get());
    }

    @Test
    public void testOnSnapshotLoad() {
        AtomicBoolean reached = new AtomicBoolean();

        DelegatingStateMachine sm = new JraftServerImpl.DelegatingStateMachine(
                nodeId(),
                TEST_LISTENER,
                mock(NodeOptions.class),
                testFailureManager(reached)
        );

        SnapshotReader reader = mock(SnapshotReader.class);

        when(reader.getPath()).thenReturn("");

        sm.onSnapshotLoad(reader);

        assertTrue(reached.get());
    }

    private static FailureManager testFailureManager(AtomicBoolean reached) {
        return new FailureManager(new FailureHandler() {
            @Override
            public boolean onFailure(FailureContext failureCtx) {
                assertEquals(EXPECTED_ERROR, failureCtx.error());

                reached.set(true);
                return false;
            }

            @Override
            public Set<FailureType> ignoredFailureTypes() {
                return Set.of();
            }
        });
    }

    private static RaftNodeId nodeId() {
        return new RaftNodeId(new ZonePartitionId(0, 0), new Peer("test"));
    }
}
