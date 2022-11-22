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

package org.apache.ignite.internal.metastorage;

import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.when;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.ignite.internal.metastorage.client.MetaStorageService;
import org.apache.ignite.internal.metastorage.client.MetaStorageServiceImpl;
import org.apache.ignite.internal.metastorage.server.Entry;
import org.apache.ignite.internal.metastorage.server.KeyValueStorage;
import org.apache.ignite.internal.metastorage.server.raft.MetaStorageListener;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.lang.ByteArray;
import org.apache.ignite.raft.client.Command;
import org.apache.ignite.raft.client.ReadCommand;
import org.apache.ignite.raft.client.WriteCommand;
import org.apache.ignite.raft.client.service.CommandClosure;
import org.apache.ignite.raft.client.service.RaftGroupService;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Test for cursor created by {@link MetaStorageService#range(ByteArray, ByteArray)} commands.
 */
@ExtendWith(MockitoExtension.class)
public class MetaStorageRangeCursorTest {
    MetaStorageListener listener;

    @Mock
    RaftGroupService raftGroupService;

    @Mock
    KeyValueStorage storage;

    @ParameterizedTest
    @MethodSource("getParameters")
    public void testCursor(int elementsCount, int keyTo) {
        int limit = Math.min(keyTo, elementsCount);

        when(storage.range(any(), any(), anyBoolean())).thenReturn(new TestCursor(iterator(limit)));
        when(storage.range(any(), any(), anyLong(), anyBoolean())).thenReturn(new TestCursor(iterator(limit)));

        listener = new MetaStorageListener(storage);

        when(raftGroupService.run(any())).thenAnswer(invocation -> runCommand(invocation.getArgument(0)));

        MetaStorageService metaStorageService = new MetaStorageServiceImpl(raftGroupService, "test", "test");

        checkCursor(metaStorageService.range(intToBytes(0), intToBytes(keyTo)), limit);
        checkCursor(metaStorageService.range(intToBytes(0), intToBytes(keyTo), 0), limit);
    }

    private static Stream<Arguments> getParameters() {
        List<Arguments> args = new ArrayList<>();

        List<Integer> elements = asList(0, 1, 10, 99, 100, 101, 150, 200, 201, 250);
        List<Integer> keysTo = asList(10, 100, 1000);

        for (Integer e : elements) {
            for (Integer k : keysTo) {
                args.add(Arguments.of(e, k));
            }
        }

        return args.stream();
    }

    private void checkCursor(Cursor<org.apache.ignite.internal.metastorage.client.Entry> range, int count) {
        for (int i = 0; i < count; i++) {
            String errorDetails = "count=" + count + ", i=" + i;

            assertTrue(range.hasNext(), errorDetails);

            org.apache.ignite.internal.metastorage.client.Entry e = range.next();

            assertEquals(intToBytes(i), e.key(), errorDetails);
            assertArrayEquals(intToBytes(i).bytes(), e.value(), errorDetails);
            assertEquals(i, e.revision(), errorDetails);
            assertEquals(i, e.updateCounter(), errorDetails);
        }

        assertFalse(range.hasNext());
    }

    private Iterator<Entry> iterator(int elementsCount) {
        return IntStream.range(0, elementsCount).mapToObj(this::intToEntry).iterator();
    }

    private Entry intToEntry(int i) {
        return new Entry(intToBytes(i).bytes(), intToBytes(i).bytes(), i, i);
    }

    private ByteArray intToBytes(int i) {
        byte[] bytes = new byte[4];

        ByteBuffer.wrap(bytes).putInt(i);

        return new ByteArray(bytes);
    }

    private CompletableFuture<Serializable> runCommand(Command command) {
        AtomicReference<CompletableFuture<Serializable>> resRef = new AtomicReference<>();

        CommandClosure<? extends Command> closure = new CommandClosure<>() {
            @Override public Command command() {
                return command;
            }

            @Override public void result(@Nullable Serializable res) {
                resRef.set(res instanceof Throwable ? failedFuture((Throwable) res) : completedFuture(res));
            }
        };

        if (command instanceof ReadCommand) {
            listener.onRead(singleton((CommandClosure<ReadCommand>) closure).iterator());
        } else {
            listener.onWrite(singleton((CommandClosure<WriteCommand>) closure).iterator());
        }

        return resRef.get();
    }

    private static class TestCursor<T> implements Cursor<T> {
        final Iterator<T> iterator;

        private TestCursor(Iterator<T> iterator) {
            this.iterator = iterator;
        }

        @Override public void close() {
            // No-op.
        }

        @Override public boolean hasNext() {
            return iterator.hasNext();
        }

        @Override public T next() {
            return iterator.next();
        }
    }
}
