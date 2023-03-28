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

package org.apache.ignite.internal.sql.engine;

import static java.util.stream.Collectors.joining;
import static org.apache.ignite.internal.sql.engine.util.QueryChecker.containsIndexScan;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.raft.Peer;
import org.apache.ignite.internal.raft.service.RaftGroupService;
import org.apache.ignite.internal.schema.configuration.TablesConfiguration;
import org.apache.ignite.internal.storage.index.IndexStorage;
import org.apache.ignite.internal.table.InternalTable;
import org.apache.ignite.internal.table.TableImpl;
import org.apache.ignite.lang.IgniteStringFormatter;
import org.apache.ignite.table.Table;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Integration test of index building.
 */
public class ItBuildIndexTest extends ClusterPerClassIntegrationTest {
    private static final String TABLE_NAME = "test_table";

    private static final String INDEX_NAME = "test_index";

    @AfterEach
    void tearDown() {
        sql("DROP TABLE IF EXISTS " + TABLE_NAME);
    }

    @ParameterizedTest
    @MethodSource("replicas")
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-19085")
    void testBuildIndexOnStableTopology(int replicas) throws Exception {
        sql(IgniteStringFormatter.format(
                "CREATE TABLE {} (i0 INTEGER PRIMARY KEY, i1 INTEGER) WITH replicas={}, partitions={}",
                TABLE_NAME, replicas, 2
        ));

        sql(IgniteStringFormatter.format(
                "INSERT INTO {} VALUES {}",
                TABLE_NAME, toValuesString(List.of(1, 1), List.of(2, 2), List.of(3, 3), List.of(4, 4), List.of(5, 5))
        ));

        sql(IgniteStringFormatter.format("CREATE INDEX {} ON {} (i1)", INDEX_NAME, TABLE_NAME));

        // FIXME: IGNITE-18733
        waitForIndex(INDEX_NAME);

        waitForIndexBuild(TABLE_NAME, INDEX_NAME);

        assertQuery(IgniteStringFormatter.format("SELECT * FROM {} WHERE i1 > 0", TABLE_NAME))
                .matches(containsIndexScan("PUBLIC", TABLE_NAME.toUpperCase(), INDEX_NAME.toUpperCase()))
                .returns(1, 1)
                .returns(2, 2)
                .returns(3, 3)
                .returns(4, 4)
                .returns(5, 5)
                .check();
    }

    private static int[] replicas() {
        // FIXME: IGNITE-19086 Fix NullPointerException on insertAll
        //        return new int[]{1, 2, 3};
        return new int[]{1};
    }

    private static String toValuesString(List<Object>... values) {
        return Stream.of(values)
                .peek(Assertions::assertNotNull)
                .map(objects -> objects.stream().map(Object::toString).collect(joining(", ", "(", ")")))
                .collect(joining(", "));
    }

    private void waitForIndexBuild(String tableName, String indexName) throws Exception {
        for (Ignite clusterNode : CLUSTER_NODES) {
            CompletableFuture<Table> tableFuture = clusterNode.tables().tableAsync(tableName);

            assertThat(tableFuture, willCompleteSuccessfully());

            TableImpl tableImpl = (TableImpl) tableFuture.join();

            InternalTable internalTable = tableImpl.internalTable();

            UUID indexId = ((IgniteImpl) clusterNode).clusterConfiguration()
                    .getConfiguration(TablesConfiguration.KEY)
                    .indexes()
                    .get(INDEX_NAME.toUpperCase())
                    .id()
                    .value();

            assertNotNull(indexId, "table=" + tableName + ", index=" + indexName);

            for (int partitionId = 0; partitionId < internalTable.partitions(); partitionId++) {
                RaftGroupService raftGroupService = internalTable.partitionRaftGroupService(partitionId);

                Stream<Peer> allPeers = Stream.concat(Stream.of(raftGroupService.leader()), raftGroupService.peers().stream());

                if (allPeers.map(Peer::consistentId).noneMatch(clusterNode.name()::equals)) {
                    continue;
                }

                IndexStorage index = internalTable.storage().getOrCreateIndex(partitionId, indexId);

                assertTrue(waitForCondition(() -> index.getNextRowIdToBuild() == null, 10, 10_000));
            }
        }
    }
}
