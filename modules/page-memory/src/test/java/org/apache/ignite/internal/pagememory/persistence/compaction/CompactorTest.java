/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.pagememory.persistence.compaction;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.runAsync;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.util.GridUnsafe.bufferAddress;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import org.apache.ignite.configuration.ConfigurationValue;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.pagememory.io.PageIo;
import org.apache.ignite.internal.pagememory.persistence.store.DeltaFilePageStoreIo;
import org.apache.ignite.internal.pagememory.persistence.store.FilePageStore;
import org.apache.ignite.internal.pagememory.persistence.store.FilePageStoreManager;
import org.junit.jupiter.api.Test;

/**
 * For {@link Compactor} testing.
 */
public class CompactorTest {
    private static final int PAGE_SIZE = 1024;

    private final IgniteLogger log = Loggers.forClass(CompactorTest.class);

    @Test
    void testStartAndStop() throws Exception {
        Compactor compactor = new Compactor(log, "test", null, threadsConfig(1), mock(FilePageStoreManager.class), PAGE_SIZE);

        compactor.start();

        assertNull(compactor.runner());

        assertFalse(compactor.isCancelled());
        assertFalse(compactor.isDone());
        assertFalse(Thread.currentThread().isInterrupted());

        compactor.start();

        assertTrue(waitForCondition(() -> compactor.runner() != null, 10, 1_000));

        compactor.stop();

        assertTrue(waitForCondition(() -> compactor.runner() == null, 10, 1_000));

        assertTrue(compactor.isCancelled());
        assertTrue(compactor.isDone());
        assertFalse(Thread.currentThread().isInterrupted());
    }

    @Test
    void testMergeDeltaFileToMainFile() throws Throwable {
        Compactor compactor = new Compactor(log, "test", null, threadsConfig(1), mock(FilePageStoreManager.class), PAGE_SIZE);

        FilePageStore filePageStore = mock(FilePageStore.class);
        DeltaFilePageStoreIo deltaFilePageStoreIo = mock(DeltaFilePageStoreIo.class);

        when(filePageStore.removeDeltaFile(eq(deltaFilePageStoreIo))).thenReturn(true);

        when(deltaFilePageStoreIo.pageIndexes()).thenReturn(new int[]{0});

        when(deltaFilePageStoreIo.readWithMergedToFilePageStoreCheck(anyLong(), anyLong(), any(ByteBuffer.class), anyBoolean()))
                .then(answer -> {
                    ByteBuffer buffer = answer.getArgument(2);

                    PageIo.setPageId(bufferAddress(buffer), 1);

                    return true;
                });

        compactor.mergeDeltaFileToMainFile(filePageStore, deltaFilePageStoreIo);

        verify(deltaFilePageStoreIo, times(1)).readWithMergedToFilePageStoreCheck(eq(0L), eq(0L), any(ByteBuffer.class), anyBoolean());
        verify(filePageStore, times(1)).write(eq(1L), any(ByteBuffer.class), anyBoolean());

        verify(filePageStore, times(1)).sync();
        verify(filePageStore, times(1)).removeDeltaFile(eq(deltaFilePageStoreIo));

        verify(deltaFilePageStoreIo, times(1)).markMergedToFilePageStore();
        verify(deltaFilePageStoreIo, times(1)).stop(eq(true));
    }

    @Test
    void testDoCompaction() throws Throwable {
        FilePageStore filePageStore = mock(FilePageStore.class);

        DeltaFilePageStoreIo deltaFilePageStoreIo = mock(DeltaFilePageStoreIo.class);

        when(filePageStore.getDeltaFileToCompaction()).thenReturn(deltaFilePageStoreIo);

        FilePageStoreManager filePageStoreManager = mock(FilePageStoreManager.class);

        when(filePageStoreManager.allPageStores()).thenReturn(List.of(List.of(filePageStore)));

        Compactor compactor = spy(new Compactor(log, "test", null, threadsConfig(1), filePageStoreManager, PAGE_SIZE));

        doAnswer(answer -> {
            assertSame(filePageStore, answer.getArgument(0));
            assertSame(deltaFilePageStoreIo, answer.getArgument(1));

            return null;
        })
                .when(compactor)
                .mergeDeltaFileToMainFile(any(FilePageStore.class), any(DeltaFilePageStoreIo.class));

        compactor.doCompaction();

        verify(filePageStore, times(1)).getDeltaFileToCompaction();

        verify(compactor, times(1)).mergeDeltaFileToMainFile(any(FilePageStore.class), any(DeltaFilePageStoreIo.class));
    }

    @Test
    void testBody() throws Exception {
        Compactor compactor = spy(new Compactor(log, "test", null, threadsConfig(1), mock(FilePageStoreManager.class), PAGE_SIZE));

        doNothing().when(compactor).waitDeltaFiles();

        doAnswer(answer -> {
            compactor.cancel();

            return null;
        }).when(compactor).doCompaction();

        compactor.body();

        verify(compactor, times(3)).isCancelled();
        verify(compactor, times(1)).waitDeltaFiles();
        verify(compactor, times(1)).doCompaction();
    }

    @Test
    void testWaitDeltaFiles() throws Exception {
        Compactor compactor = spy(new Compactor(log, "test", null, threadsConfig(1), mock(FilePageStoreManager.class), PAGE_SIZE));

        CompletableFuture<?> waitDeltaFilesFuture = runAsync(compactor::waitDeltaFiles);

        assertThrows(TimeoutException.class, () -> waitDeltaFilesFuture.get(100, MILLISECONDS));

        compactor.addDeltaFiles(1);

        waitDeltaFilesFuture.get(100, MILLISECONDS);
    }

    @Test
    void testCancel() throws Exception {
        Compactor compactor = spy(new Compactor(log, "test", null, threadsConfig(1), mock(FilePageStoreManager.class), PAGE_SIZE));

        assertFalse(compactor.isCancelled());

        CompletableFuture<?> waitDeltaFilesFuture = runAsync(compactor::waitDeltaFiles);

        assertThrows(TimeoutException.class, () -> waitDeltaFilesFuture.get(100, MILLISECONDS));

        compactor.cancel();

        assertTrue(compactor.isCancelled());
        assertFalse(Thread.currentThread().isInterrupted());

        waitDeltaFilesFuture.get(100, MILLISECONDS);
    }

    private static ConfigurationValue<Integer> threadsConfig(int threads) {
        ConfigurationValue<Integer> configValue = mock(ConfigurationValue.class);

        when(configValue.value()).thenReturn(threads);

        return configValue;
    }
}
