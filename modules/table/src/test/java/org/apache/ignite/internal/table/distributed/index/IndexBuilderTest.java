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

package org.apache.ignite.internal.table.distributed.index;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willTimeoutFast;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.IntStream;
import org.apache.ignite.internal.replicator.ReplicaService;
import org.apache.ignite.internal.replicator.message.ReplicaRequest;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.index.IndexStorage;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.network.ClusterNode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** For {@link IndexBuilder} testing. */
public class IndexBuilderTest extends BaseIgniteAbstractTest {
    private static final int TABLE_ID = 1;

    private static final int INDEX_ID = 2;

    private static final int PARTITION_ID = 3;

    private final ReplicaService replicaService = mock(ReplicaService.class, invocation -> completedFuture(null));

    private IndexBuilder indexBuilder;

    @BeforeEach
    void setUp() {
        indexBuilder = new IndexBuilder("test", 1, replicaService);
    }

    @AfterEach
    void tearDown() {
        indexBuilder.close();
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

        assertFalse(listenCompletionIndexBuildingFuture.newIncompleteFuture().isDone());

        secondInvokeReplicaServiceFuture.complete(null);

        assertThat(listenCompletionIndexBuildingFuture, willCompleteSuccessfully());
    }

    @Test
    void testIndexBuildCompletionListenerForAlreadyBuiltIndex() {
        CompletableFuture<Void> listenCompletionIndexBuildingFuture = listenCompletionIndexBuilding(INDEX_ID, TABLE_ID, PARTITION_ID);

        scheduleBuildIndex(INDEX_ID, TABLE_ID, PARTITION_ID, List.of());

        assertThat(listenCompletionIndexBuildingFuture, willTimeoutFast());
    }

    private void scheduleBuildIndex(int indexId, int tableId, int partitionId, Collection<RowId> nextRowIdsToBuild) {
        indexBuilder.scheduleBuildIndex(
                tableId,
                partitionId,
                indexId,
                indexStorage(nextRowIdsToBuild),
                mock(MvPartitionStorage.class),
                mock(ClusterNode.class)
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
                .thenReturn(completedFuture(null))
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
