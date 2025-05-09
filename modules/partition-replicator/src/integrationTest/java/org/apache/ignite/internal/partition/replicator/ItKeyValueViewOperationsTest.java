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

package org.apache.ignite.internal.partition.replicator;

import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.partitiondistribution.PartitionDistributionUtils.calculateAssignmentForPartition;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import org.apache.ignite.internal.partition.replicator.fixtures.Node;
import org.apache.ignite.internal.partitiondistribution.Assignment;
import org.apache.ignite.internal.table.TableTestUtils;
import org.apache.ignite.lang.NullableValue;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.tx.Transaction;
import org.junit.jupiter.api.Test;

/**
 * KeyValueView operations tests within colocation track.
 */
// TODO: https://issues.apache.org/jira/browse/IGNITE-22522 remove this test after the switching to zone-based replication
public class ItKeyValueViewOperationsTest extends ItAbstractColocationTest {
    private Node node;

    @Test
    public void testKvOperations() throws Exception {
        startCluster(3);

        Assignment replicaAssignment = (Assignment) calculateAssignmentForPartition(
                cluster.stream().map(n -> n.name).collect(toList()), 0, 1, 1, 1).toArray()[0];

        node = getNode(replicaAssignment.consistentId());

        createZone(node, "test_zone", 1, 1);

        createTable(node, "test_zone", "test_table");
        int tableId = TableTestUtils.getTableId(node.catalogManager, "test_table", node.hybridClock.nowLong());

        KeyValueView<Long, Integer> keyValueView = node.tableManager.table(tableId).keyValueView(Long.class, Integer.class);

        // put
        assertOperationsWithinTransaction(
                tx -> {
                    assertDoesNotThrow(() -> keyValueView.put(tx, 0L, 0));
                    assertDoesNotThrow(() -> keyValueView.put(tx, 1L, 1));
                }
        );

        // contains
        assertOperationsWithinTransaction(
                tx -> {
                    assertTrue(keyValueView.contains(tx, 0L));
                    assertTrue(keyValueView.contains(tx, 1L));
                    assertFalse(keyValueView.contains(tx, 3L));
                }
        );

        // putAll
        assertOperationsWithinTransaction(
                tx -> {
                    Map<Long, Integer> valuesToPut = Map.of(2L, 2, 3L, 3, 30L, 30, 100L, 100,
                            200L, 200, 300L, 300, 400L, 400, 500L, 500, 600L, 600);
                    assertDoesNotThrow(() -> keyValueView.putAll(tx, valuesToPut));
                }
        );

        // containsAll
        assertOperationsWithinTransaction(
                tx -> {
                    assertTrue(keyValueView.containsAll(tx, Arrays.asList(2L, 3L)));
                    assertFalse(keyValueView.containsAll(tx, Arrays.asList(3L, 4L)));
                }
        );

        // getAndPut
        assertOperationsWithinTransaction(
                tx -> assertEquals(2, keyValueView.getAndPut(tx, 2L, 22))
        );

        // getNullableAndPut
        assertOperationsWithinTransaction(
                tx -> {
                    assertEquals(NullableValue.of(22), keyValueView.getNullableAndPut(tx, 2L, 33));
                    assertNull(keyValueView.getNullableAndPut(tx, 5L, 5));
                }
        );

        // putIfAbsent
        assertOperationsWithinTransaction(
                tx -> {
                    assertFalse(keyValueView.putIfAbsent(tx, 0L, 0));
                    assertTrue(keyValueView.putIfAbsent(tx, 6L, 6));
                }
        );

        // remove
        assertOperationsWithinTransaction(
                tx -> {
                    assertTrue(keyValueView.remove(tx, 6L));
                    assertFalse(keyValueView.remove(tx, 7L));
                    assertTrue(keyValueView.remove(tx, 0L, 0));
                    assertFalse(keyValueView.remove(tx, 1L, 2));
                }
        );

        // removeAll
        assertOperationsWithinTransaction(
                tx -> {
                    Set<Long> keysToRemove = Set.of(30L, 50L);
                    assertEquals(singletonList(50L), keyValueView.removeAll(tx, keysToRemove));
                }
        );

        // getAndRemove
        assertOperationsWithinTransaction(
                tx -> {
                    assertEquals(100, keyValueView.getAndRemove(tx, 100L));
                    assertNull(keyValueView.getAndRemove(tx, 1000L));
                }
        );

        // getNullableAndRemove
        assertOperationsWithinTransaction(
                tx -> {
                    assertEquals(NullableValue.of(200), keyValueView.getNullableAndRemove(tx, 200L));
                    assertNull(keyValueView.getNullableAndRemove(tx, 1000L));
                }
        );

        // replace
        assertOperationsWithinTransaction(
                tx -> {
                    assertTrue(keyValueView.replace(tx, 300L, 330));
                    assertFalse(keyValueView.replace(tx, 200L, 230));
                    assertTrue(keyValueView.replace(tx, 400L, 400, 450));
                    assertFalse(keyValueView.replace(tx, 500L, 510, 520));
                }
        );

        // getAndReplace
        assertOperationsWithinTransaction(
                tx -> {
                    assertEquals(500, keyValueView.getAndReplace(tx, 500L, 600));
                    assertNull(keyValueView.getAndReplace(tx, 1000L, 230));
                }
        );

        // getNullableAndReplace
        assertOperationsWithinTransaction(
                tx -> {
                    assertEquals(NullableValue.of(600), keyValueView.getNullableAndReplace(tx, 600L, 700));
                    assertNull(keyValueView.getNullableAndReplace(tx, 1000L, 230));
                }
        );

        // Variety of get methods
        assertOperationsWithinTransaction(
                tx -> {
                    assertEquals(700, keyValueView.get(tx, 600L));
                    assertEquals(NullableValue.of(700), keyValueView.getNullable(tx, 600L));
                    assertNull(keyValueView.getNullable(tx, 1000L));
                    assertEquals(700, keyValueView.getOrDefault(tx, 600L, 1111));
                    assertEquals(1111, keyValueView.getOrDefault(tx, 1000L, 1111));
                    assertEquals(Map.of(600L, 700), keyValueView.getAll(tx, singletonList(600L)));
                }
        );
    }

    private void assertOperationsWithinTransaction(Consumer<Transaction> operationsToRun) {
        // Usually in colocation tests we use runInTransaction or implicit transactions, thus for greater variety, explicit ones are used.
        Transaction tx = node.transactions().begin();
        operationsToRun.accept(tx);
        tx.commit();
    }
}
