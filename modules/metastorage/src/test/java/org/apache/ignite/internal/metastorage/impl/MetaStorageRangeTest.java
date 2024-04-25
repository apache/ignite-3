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

import static java.util.stream.Collectors.toCollection;
import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.testframework.flow.TestFlowUtils.subscribeToList;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.util.ByteUtils.intToBytes;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.IntStream;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.ByteArray;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.server.KeyValueStorage;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.internal.util.ByteUtils;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Test for cursor created by {@link MetaStorageService#range(ByteArray, ByteArray)} commands.
 */
@ExtendWith(WorkDirectoryExtension.class)
public abstract class MetaStorageRangeTest extends BaseIgniteAbstractTest {
    private KeyValueStorage storage;

    private MetaStorageManager metaStorageManager;

    abstract KeyValueStorage getStorage(Path path);

    @BeforeEach
    void setUp(@WorkDirectory Path workDir) {
        storage = spy(getStorage(workDir));

        metaStorageManager = StandaloneMetaStorageManager.create(storage);

        assertThat(metaStorageManager.startAsync(), willCompleteSuccessfully());
    }

    @AfterEach
    void tearDown() {
        metaStorageManager.beforeNodeStop();
        assertThat(metaStorageManager.stopAsync(), willCompleteSuccessfully());
    }

    @Test
    void testEmptyRange() {
        assertThat(range(ByteArray.fromString("bar"), ByteArray.fromString("foo")), willBe(empty()));
    }

    @Test
    void testEmptyPrefix() {
        assertThat(prefix(ByteArray.fromString("foo")), willBe(empty()));
    }

    @Test
    void testRangePagination() {
        List<byte[]> keys = IntStream.range(0, MetaStorageServiceImpl.BATCH_SIZE * 2)
                .mapToObj(ByteUtils::intToBytes)
                .collect(toList());

        storage.putAll(keys, keys, HybridTimestamp.MIN_VALUE);

        List<Entry> expectedEntries = getAll(keys);

        byte[] middleKeyFirstPage = keys.get(MetaStorageServiceImpl.BATCH_SIZE / 2);

        assertThat(
                range(new ByteArray(intToBytes(0)), new ByteArray(middleKeyFirstPage)),
                willBe(expectedEntries.subList(0, MetaStorageServiceImpl.BATCH_SIZE / 2))
        );

        verify(storage).range(any(), any());

        byte[] lastKeyFirstPage = keys.get(MetaStorageServiceImpl.BATCH_SIZE);

        assertThat(
                range(new ByteArray(intToBytes(0)), new ByteArray(lastKeyFirstPage)),
                willBe(expectedEntries.subList(0, MetaStorageServiceImpl.BATCH_SIZE))
        );

        verify(storage, times(2)).range(any(), any());

        byte[] middleKeySecondPage = keys.get(MetaStorageServiceImpl.BATCH_SIZE + MetaStorageServiceImpl.BATCH_SIZE / 2);

        assertThat(
                range(new ByteArray(intToBytes(0)), new ByteArray(middleKeySecondPage)),
                willBe(expectedEntries.subList(0, MetaStorageServiceImpl.BATCH_SIZE + MetaStorageServiceImpl.BATCH_SIZE / 2))
        );

        verify(storage, times(4)).range(any(), any());

        assertThat(
                range(new ByteArray(intToBytes(0)), null),
                willBe(expectedEntries)
        );

        verify(storage, times(6)).range(any(), any());
    }

    @Test
    void testPrefixPaginationSinglePage() {
        List<byte[]> keys = IntStream.range(0, MetaStorageServiceImpl.BATCH_SIZE)
                .mapToObj(i -> "foo" + i)
                .sorted()
                .map(s -> s.getBytes(StandardCharsets.UTF_8))
                .collect(toList());

        storage.putAll(keys, keys, HybridTimestamp.MIN_VALUE);

        assertThat(prefix(ByteArray.fromString("foo")), willBe(getAll(keys)));

        verify(storage, times(1)).range(any(), any());
    }

    @Test
    void testPrefixPaginationTwoPages() {
        List<byte[]> keys = IntStream.range(0, MetaStorageServiceImpl.BATCH_SIZE * 2)
                .mapToObj(i -> "foo" + i)
                .sorted()
                .map(s -> s.getBytes(StandardCharsets.UTF_8))
                .collect(toList());

        storage.putAll(keys, keys, HybridTimestamp.MIN_VALUE);

        assertThat(prefix(ByteArray.fromString("foo")), willBe(getAll(keys)));

        verify(storage, times(2)).range(any(), any());
    }

    private CompletableFuture<List<Entry>> range(ByteArray from, @Nullable ByteArray to) {
        return subscribeToList(metaStorageManager.range(from, to));
    }

    private CompletableFuture<List<Entry>> prefix(ByteArray prefix) {
        return subscribeToList(metaStorageManager.prefix(prefix));
    }

    private List<Entry> getAll(List<byte[]> keys) {
        Set<ByteArray> keyByteArrays = keys.stream()
                .map(ByteArray::new)
                .collect(toCollection(LinkedHashSet::new));

        CompletableFuture<Map<ByteArray, Entry>> entriesMapFuture = metaStorageManager.getAll(keyByteArrays);

        assertThat(entriesMapFuture, willCompleteSuccessfully());

        return keyByteArrays.stream()
                .map(entriesMapFuture.join()::get)
                .collect(toList());
    }
}
