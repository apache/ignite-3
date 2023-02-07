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

package org.apache.ignite.internal.metastorage.impl;

import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static java.util.stream.Collectors.toUnmodifiableList;
import static org.apache.ignite.internal.testframework.flow.TestFlowUtils.subscribeToList;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.when;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.server.KeyValueStorage;
import org.apache.ignite.internal.metastorage.server.raft.MetaStorageListener;
import org.apache.ignite.internal.raft.Command;
import org.apache.ignite.internal.raft.ReadCommand;
import org.apache.ignite.internal.raft.WriteCommand;
import org.apache.ignite.internal.raft.service.CommandClosure;
import org.apache.ignite.internal.raft.service.RaftGroupService;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.lang.ByteArray;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.NetworkAddress;
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
    private MetaStorageListener listener;

    @Mock
    private RaftGroupService raftGroupService;

    @Mock
    private KeyValueStorage storage;

    @ParameterizedTest
    @MethodSource("getParameters")
    public void testCursor(int elementsCount, int keyTo) {
        int limit = Math.min(keyTo, elementsCount);

        List<Entry> expectedEntries = generateRange(limit);

        when(storage.range(any(), any(), anyBoolean())).thenReturn(Cursor.fromIterable(expectedEntries));
        when(storage.range(any(), any(), anyLong(), anyBoolean())).thenReturn(Cursor.fromIterable(expectedEntries));

        listener = new MetaStorageListener(storage);

        when(raftGroupService.run(any())).thenAnswer(invocation -> runCommand(invocation.getArgument(0)));

        var localNode = new ClusterNode("test", "test", new NetworkAddress("localhost", 10000));

        MetaStorageService metaStorageService = new MetaStorageServiceImpl(raftGroupService, localNode);

        checkCursor(metaStorageService.range(intToBytes(0), intToBytes(keyTo)), expectedEntries);
        checkCursor(metaStorageService.range(intToBytes(0), intToBytes(keyTo), 0), expectedEntries);
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

    private static void checkCursor(Publisher<Entry> range, List<Entry> expectedEntries) {
        CompletableFuture<List<Entry>> resultFuture = subscribeToList(range);

        assertThat(resultFuture, willBe(expectedEntries));
    }

    private static List<Entry> generateRange(int elementsCount) {
        return IntStream.range(0, elementsCount)
                .mapToObj(MetaStorageRangeCursorTest::intToEntry)
                .collect(toUnmodifiableList());
    }

    private static Entry intToEntry(int i) {
        byte[] bytes = intToBytes(i).bytes();

        return new EntryImpl(bytes, bytes, i, i);
    }

    private static ByteArray intToBytes(int i) {
        return new ByteArray(ByteBuffer.allocate(Integer.BYTES).putInt(i).array());
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
}
