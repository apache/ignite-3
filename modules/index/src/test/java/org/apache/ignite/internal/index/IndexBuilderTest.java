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

package org.apache.ignite.internal.index;

import static java.util.concurrent.CompletableFuture.failedFuture;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willTimeoutFast;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.internal.util.IgniteUtils.closeAll;
import static org.apache.ignite.internal.util.IgniteUtils.shutdownAndAwaitTermination;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import org.apache.ignite.internal.replicator.ReplicaService;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.replicator.exception.ReplicationTimeoutException;
import org.apache.ignite.internal.replicator.message.ReplicaRequest;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.index.IndexStorage;
import org.apache.ignite.internal.table.distributed.replication.request.BuildIndexReplicaRequest;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.network.ClusterNode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/** For {@link IndexBuilder} testing. */
public class IndexBuilderTest extends BaseIgniteAbstractTest {
    private static final int TABLE_ID = 1;

    private static final int INDEX_ID = 2;

    private static final int PARTITION_ID = 3;

    private static final long ANY_ENLISTMENT_CONSISTENCY_TOKEN = 100500;

    private static final int ANY_INDEX_CREATION_CATALOG_VERSION = 1;

    private final ReplicaService replicaService = mock(ReplicaService.class, invocation -> nullCompletedFuture());

    private final ExecutorService executorService = newSingleThreadExecutor();

    private final IndexBuilder indexBuilder = new IndexBuilder(executorService, replicaService);

    @AfterEach
    void tearDown() throws Exception {
        closeAll(
                indexBuilder::close,
                () -> shutdownAndAwaitTermination(executorService, 1, TimeUnit.SECONDS)
        );
    }

    @Test
    void testIndexBuildCompletionListener() {
        CompletableFuture<Void> listenCompletionIndexBuildingFuture = listenCompletionIndexBuilding(INDEX_ID, TABLE_ID, PARTITION_ID);

        scheduleBuildIndex(INDEX_ID, TABLE_ID, PARTITION_ID, List.of(rowId(PARTITION_ID)));

        assertThat(listenCompletionIndexBuildingFuture, willCompleteSuccessfully());
    }

    @Test
    void testStopListenIndexBuildCompletion() {
        CompletableFuture<Void> invokeListenerFuture = new CompletableFuture<>();

        IndexBuildCompletionListener listener = (indexId, tableId, partitionId) -> invokeListenerFuture.complete(null);

        indexBuilder.listen(listener);
        indexBuilder.stopListen(listener);

        scheduleBuildIndex(INDEX_ID, TABLE_ID, PARTITION_ID, List.of(rowId(PARTITION_ID)));

        assertThat(invokeListenerFuture, willTimeoutFast());
    }

    @Test
    void testIndexBuildCompletionListenerTwoBatches() {
        CompletableFuture<Void> listenCompletionIndexBuildingFuture = listenCompletionIndexBuilding(INDEX_ID, TABLE_ID, PARTITION_ID);

        List<RowId> nextRowIdsToBuild = IntStream.range(0, 2 * IndexBuilder.BATCH_SIZE)
                .mapToObj(i -> rowId(PARTITION_ID))
                .collect(toList());

        CompletableFuture<Void> secondInvokeReplicaServiceFuture = new CompletableFuture<>();

        CompletableFuture<Void> awaitSecondInvokeForReplicaService = awaitSecondInvokeForReplicaService(secondInvokeReplicaServiceFuture);

        scheduleBuildIndex(INDEX_ID, TABLE_ID, PARTITION_ID, nextRowIdsToBuild);

        assertThat(awaitSecondInvokeForReplicaService, willCompleteSuccessfully());

        assertFalse(listenCompletionIndexBuildingFuture.isDone());

        secondInvokeReplicaServiceFuture.complete(null);

        assertThat(listenCompletionIndexBuildingFuture, willCompleteSuccessfully());
    }

    @Test
    void testIndexBuildCompletionListenerForAlreadyBuiltIndex() {
        CompletableFuture<Void> listenCompletionIndexBuildingFuture = listenCompletionIndexBuilding(INDEX_ID, TABLE_ID, PARTITION_ID);

        scheduleBuildIndex(INDEX_ID, TABLE_ID, PARTITION_ID, List.of());

        assertThat(listenCompletionIndexBuildingFuture, willCompleteSuccessfully());
    }

    @Test
    void testIndexBuildWithReplicationTimeoutException() {
        CompletableFuture<Void> listenCompletionIndexBuildingFuture = listenCompletionIndexBuilding(INDEX_ID, TABLE_ID, PARTITION_ID);

        when(replicaService.invoke(any(ClusterNode.class), any()))
                .thenReturn(failedFuture(new ReplicationTimeoutException(new TablePartitionId(TABLE_ID, PARTITION_ID))))
                .thenReturn(nullCompletedFuture());

        scheduleBuildIndex(INDEX_ID, TABLE_ID, PARTITION_ID, List.of(rowId(PARTITION_ID)));

        assertThat(listenCompletionIndexBuildingFuture, willCompleteSuccessfully());

        verify(replicaService, times(2)).invoke(any(ClusterNode.class), any(BuildIndexReplicaRequest.class));
    }

    private void scheduleBuildIndex(int indexId, int tableId, int partitionId, Collection<RowId> nextRowIdsToBuild) {
        indexBuilder.scheduleBuildIndex(
                tableId,
                partitionId,
                indexId,
                indexStorage(nextRowIdsToBuild),
                mock(MvPartitionStorage.class),
                mock(ClusterNode.class),
                ANY_ENLISTMENT_CONSISTENCY_TOKEN,
                ANY_INDEX_CREATION_CATALOG_VERSION
        );
    }

    private CompletableFuture<Void> listenCompletionIndexBuilding(int indexId, int tableId, int partitionId) {
        CompletableFuture<Void> future = new CompletableFuture<>();

        indexBuilder.listen((indexId1, tableId1, partitionId1) -> {
            if (indexId1 == indexId && tableId1 == tableId && partitionId1 == partitionId) {
                future.complete(null);
            }
        });

        return future;
    }

    private CompletableFuture<Void> awaitSecondInvokeForReplicaService(CompletableFuture<Void> secondInvokeFuture) {
        CompletableFuture<Void> future = new CompletableFuture<>();

        when(replicaService.invoke(any(ClusterNode.class), any(ReplicaRequest.class)))
                .thenReturn(nullCompletedFuture())
                .thenAnswer(invocation -> {
                    future.complete(null);

                    return secondInvokeFuture;
                });

        return future;
    }

    private static IndexStorage indexStorage(Collection<RowId> nextRowIdsToBuild) {
        Iterator<RowId> it = nextRowIdsToBuild.iterator();

        IndexStorage indexStorage = mock(IndexStorage.class);

        when(indexStorage.getNextRowIdToBuild()).then(invocation -> it.hasNext() ? it.next() : null);

        return indexStorage;
    }

    private static RowId rowId(int partitionId) {
        return new RowId(partitionId);
    }
}
