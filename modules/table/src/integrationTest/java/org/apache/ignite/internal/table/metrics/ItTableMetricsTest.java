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

package org.apache.ignite.internal.table.metrics;

import static java.util.List.of;
import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.TestWrappers.unwrapIgniteImpl;
import static org.apache.ignite.internal.table.metrics.TableMetricSource.RO_READS;
import static org.apache.ignite.internal.table.metrics.TableMetricSource.RW_READS;
import static org.apache.ignite.internal.table.metrics.TableMetricSource.WRITES;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import org.apache.ignite.internal.ClusterPerClassIntegrationTest;
import org.apache.ignite.internal.metrics.LongMetric;
import org.apache.ignite.internal.metrics.Metric;
import org.apache.ignite.internal.metrics.MetricSet;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.QualifiedName;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.tx.Transaction;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * Tests key-value and record view operations metrics.
 */
public class ItTableMetricsTest extends ClusterPerClassIntegrationTest {
    private static final String TABLE_NAME = "test_table_name".toUpperCase();

    private static final String SORTED_IDX = "SORTED_IDX";
    private static final String HASH_IDX = "HASH_IDX";

    @Override
    protected int initialNodes() {
        return 2;
    }

    @BeforeAll
    void createTable() throws Exception {
        sql("CREATE TABLE " + TABLE_NAME + " (id INT PRIMARY KEY, val VARCHAR)");

        sql("CREATE INDEX IF NOT EXISTS " + SORTED_IDX + " ON PUBLIC." + TABLE_NAME + " USING SORTED (id)");
        sql("CREATE INDEX IF NOT EXISTS " + HASH_IDX + " ON PUBLIC." + TABLE_NAME + " USING HASH (val)");
    }

    /**
     * Returns a key value view for the given table.
     *
     * @param tableName Table name.
     * @param nodeIndex Node index to create a key value view.
     * @return Key value view.
     */
    private static KeyValueView<Integer, String> keyValueView(String tableName, int nodeIndex) {
        return CLUSTER.node(nodeIndex).tables().table(tableName).keyValueView(Integer.class, String.class);
    }

    /**
     * Returns a record view for the given table.
     *
     * @param tableName Table name.
     * @param nodeIndex Node index to create a key value view.
     * @return Record view.
     */
    private static RecordView<Tuple> recordView(String tableName, int nodeIndex) {
        return CLUSTER.node(nodeIndex).tables().table(tableName).recordView();
    }

    @Test
    void get() {
        // Implicit read-only transaction.
        testKeyValueViewOperation(TABLE_NAME, RO_READS, 1, view -> view.get(null, 12));

        // Explicit read-write transaction.
        testKeyValueViewOperation(TABLE_NAME, RW_READS, 1, view -> {
            Transaction tx = node(0).transactions().begin();

            view.get(tx, 12);

            tx.commit();
        });
    }

    @Test
    void getAll() {
        List<Integer> keys = of(12, 15, 17, 19, 23);

        // Implicit getAll operation starts a read-write transaction when all keys are not mapped to the same partition.
        testKeyValueViewOperation(TABLE_NAME, RW_READS, keys.size(), view -> view.getAll(null, keys));

        // Single key getAll operation starts a read-only transaction.
        List<Integer> roKeys = of(12);
        testKeyValueViewOperation(TABLE_NAME, RO_READS, 1, view -> view.getAll(null, roKeys));

        List<Integer> nonUniqueKeys = of(12, 15, 12);
        testKeyValueViewOperation(TABLE_NAME, RW_READS, nonUniqueKeys.size(), view -> view.getAll(null, nonUniqueKeys));
    }

    @Test
    void getOrDefault() {
        KeyValueView<Integer, String> kvView = keyValueView(TABLE_NAME, 0);

        Integer existingKey = 12;
        Integer nonExistingKey = -1;

        kvView.put(null, existingKey, "value_12");
        kvView.remove(null, nonExistingKey);

        testKeyValueViewOperation(TABLE_NAME, RO_READS, 2, view -> {
            view.getOrDefault(null, existingKey, "default");
            view.getOrDefault(null, nonExistingKey, "default");
        });

        testKeyValueViewOperation(TABLE_NAME, RW_READS, 2, view -> {
            Transaction tx = node(0).transactions().begin();

            view.getOrDefault(tx, existingKey, "default");
            view.getOrDefault(tx, nonExistingKey, "default");

            tx.commit();
        });
    }

    @Test
    void contains() {
        testKeyValueViewOperation(TABLE_NAME, RO_READS, 1, view -> view.contains(null, 12));

        testKeyValueViewOperation(TABLE_NAME, RW_READS, 1, view -> {
            Transaction tx = node(0).transactions().begin();

            view.contains(tx, 12);

            tx.commit();
        });
    }

    @Test
    void containsAll() {
        List<Integer> keys = of(12, 15, 17, 19, 23);

        // Implicit containsAll operation starts a read-write transaction when all keys are not mapped to the same partition.
        testKeyValueViewOperation(TABLE_NAME, RW_READS, keys.size(), view -> view.containsAll(null, keys));

        // Single key.
        List<Integer> roKeys = of(12);
        testKeyValueViewOperation(TABLE_NAME, RO_READS, 1, view -> view.containsAll(null, roKeys));
    }

    @Test
    void put() {
        testKeyValueViewOperation(TABLE_NAME, WRITES, 1, view -> view.put(null, 42, "value_42"));
    }

    @Test
    void putAll() {
        Map<Integer, String> values = Map.of(12, "12", 15, "15", 17, "17", 19, "19", 23, "23");

        testKeyValueViewOperation(TABLE_NAME, WRITES, values.size(), view -> view.putAll(null, values));
    }

    @Test
    void getAndPut() {
        testKeyValueViewOperation(TABLE_NAME, of(RW_READS, WRITES), of(1L, 1L), view -> view.getAndPut(null, 12, "value"));
    }

    @Test
    void remove() {
        Integer key = 12;
        KeyValueView<Integer, String> kvView = keyValueView(TABLE_NAME, 0);
        kvView.put(null, key, "value_42");

        // Remove existing key.
        testKeyValueViewOperation(TABLE_NAME, WRITES, 1, view -> view.remove(null, key));

        // Remove non existing key.
        testKeyValueViewOperation(TABLE_NAME, WRITES, 0, view -> view.remove(null, key));
    }

    @Test
    void exactRemove() {
        Integer key = 12;
        KeyValueView<Integer, String> kvView = keyValueView(TABLE_NAME, 0);
        kvView.put(null, key, "value_42");

        // Remove existing key and non-matching value.
        testKeyValueViewOperation(TABLE_NAME, of(RW_READS, WRITES), of(1L, 0L), view -> view.remove(null, key, "wrong-value"));

        // Remove existing key and matching value.
        testKeyValueViewOperation(TABLE_NAME, of(RW_READS, WRITES), of(1L, 1L), view -> view.remove(null, key, "value_42"));

        // Remove non existing key.
        testKeyValueViewOperation(TABLE_NAME, of(RW_READS, WRITES), of(1L, 0L), view -> view.remove(null, key, "value_42"));
    }

    @Test
    void removeAll() {
        Map<Integer, String> values = Map.of(12, "12", 15, "15", 17, "17", 19, "19", 23, "23");

        KeyValueView<Integer, String> kvView = keyValueView(TABLE_NAME, 0);
        kvView.removeAll(null);
        kvView.putAll(null, values);

        testKeyValueViewOperation(TABLE_NAME, WRITES, values.size(), view -> view.removeAll(null));
    }

    @Test
    void removeCollectionKeys() {
        Map<Integer, String> values = Map.of(12, "12", 15, "15", 17, "17", 19, "19", 23, "23");

        KeyValueView<Integer, String> kvView = keyValueView(TABLE_NAME, 0);
        kvView.putAll(null, values);

        // Remove existing keys.
        testKeyValueViewOperation(TABLE_NAME, WRITES, values.size(), view -> view.removeAll(null, values.keySet()));

        // Remove non-existing keys.
        testKeyValueViewOperation(TABLE_NAME, WRITES, 0, view -> view.removeAll(null, values.keySet()));

        kvView.putAll(null, values);

        // Remove non-unique keys.
        List<Integer> nonUniqueKeys = of(12, 15, 12, 17, 19, 23);
        testKeyValueViewOperation(TABLE_NAME, WRITES, nonUniqueKeys.size() - 1, view -> view.removeAll(null, nonUniqueKeys));
    }

    @Test
    void putIfAbsent() {
        Integer key = 12;

        KeyValueView<Integer, String> kvView = keyValueView(TABLE_NAME, 0);
        kvView.remove(null, key);

        // Insert absent key.
        testKeyValueViewOperation(TABLE_NAME, of(RW_READS, WRITES), of(1L, 1L), view -> view.putIfAbsent(null, key, "value"));

        // Insert existing key.
        testKeyValueViewOperation(TABLE_NAME, of(RW_READS, WRITES), of(1L, 0L), view -> view.putIfAbsent(null, key, "value-42"));
    }

    @Test
    void getAndRemove() {
        Integer key = 12;

        KeyValueView<Integer, String> kvView = keyValueView(TABLE_NAME, 0);
        kvView.put(null, key, "value_42");

        // Remove existing key.
        testKeyValueViewOperation(TABLE_NAME, of(RW_READS, WRITES), of(1L, 1L), view -> view.getAndRemove(null, key));

        // Remove non-existing key.
        testKeyValueViewOperation(TABLE_NAME, of(RW_READS, WRITES), of(1L, 0L), view -> view.getAndRemove(null, key));
    }

    @Test
    void replace() {
        Integer key = 12;

        KeyValueView<Integer, String> kvView = keyValueView(TABLE_NAME, 0);
        kvView.put(null, key, "value");

        // Replace existing key.
        testKeyValueViewOperation(TABLE_NAME, of(RW_READS, WRITES), of(1L, 1L), view -> view.replace(null, key, "replaced"));

        kvView.remove(null, key);

        // Replace non-existing key.
        testKeyValueViewOperation(TABLE_NAME, of(RW_READS, WRITES), of(1L, 0L), view -> view.replace(null, key, "value"));
    }

    @Test
    void conditionalReplace() {
        Integer key = 12;

        KeyValueView<Integer, String> kvView = keyValueView(TABLE_NAME, 0);
        kvView.put(null, key, "value");

        // Replace existing key.
        testKeyValueViewOperation(TABLE_NAME, of(RW_READS, WRITES), of(1L, 0L), view -> view.replace(null, key, "wrong", "replaced"));
        testKeyValueViewOperation(TABLE_NAME, of(RW_READS, WRITES), of(1L, 1L), view -> view.replace(null, key, "value", "replaced"));

        kvView.remove(null, key);

        // Replace non-existing key.
        testKeyValueViewOperation(TABLE_NAME, of(RW_READS, WRITES), of(1L, 0L), view -> view.replace(null, key, "replaced", "value"));
    }

    @Test
    void getAndReplace() {
        Integer key = 12;

        KeyValueView<Integer, String> kvView = keyValueView(TABLE_NAME, 0);
        kvView.put(null, key, "value");

        // Replace existing key.
        testKeyValueViewOperation(TABLE_NAME, of(RW_READS, WRITES), of(1L, 1L), view -> view.getAndReplace(null, key, "replaced"));

        kvView.remove(null, key);

        // Replace non-existing key.
        testKeyValueViewOperation(TABLE_NAME, of(RW_READS, WRITES), of(1L, 0L), view -> view.replace(null, key, "replaced"));
    }

    @Test
    void insertAll() {
        List<Tuple> keys = of(Tuple.create().set("id", 12), Tuple.create().set("id", 42));
        List<Tuple> recs = keys.stream().map(t -> Tuple.copy(t).set("val", "value_" + t.intValue("id"))).collect(toList());

        recordView(TABLE_NAME, 0).deleteAll(null, keys);

        // Insert non-existing keys.
        testRecordViewOperation(
                TABLE_NAME,
                of(WRITES, RW_READS),
                of((long) recs.size(), (long) recs.size()),
                view -> view.insertAll(null, recs));

        // Insert existing keys.
        testRecordViewOperation(TABLE_NAME, of(WRITES, RW_READS), of(0L, (long) recs.size()), view -> view.insertAll(null, recs));

        recordView(TABLE_NAME, 0).delete(null, keys.get(0));

        // Insert one non-existing key.
        testRecordViewOperation(TABLE_NAME, of(WRITES, RW_READS), of(1L, (long) recs.size()), view -> view.insertAll(null, recs));

        // Insert non-unique keys.
        List<Tuple> nonUniqueKeys = of(
                Tuple.create().set("id", 12),
                Tuple.create().set("id", 42),
                Tuple.create().set("id", 12));
        List<Tuple> nonUniqueValues = nonUniqueKeys
                .stream()
                .map(t -> Tuple.copy(t).set("val", "value_" + t.intValue("id")))
                .collect(toList());

        recordView(TABLE_NAME, 0).deleteAll(null, keys);

        testRecordViewOperation(
                TABLE_NAME,
                of(WRITES, RW_READS),
                of((long) nonUniqueKeys.size() - 1, (long) nonUniqueKeys.size()),
                view -> view.insertAll(null, nonUniqueValues));
    }

    @Test
    void deleteAll() {
        List<Tuple> keys = of(Tuple.create().set("id", 12), Tuple.create().set("id", 42));
        List<Tuple> recs = keys.stream().map(t -> Tuple.copy(t).set("val", "value_" + t.intValue("id"))).collect(toList());

        recordView(TABLE_NAME, 0).upsertAll(null, recs);

        // Delete existing keys.
        testRecordViewOperation(TABLE_NAME, WRITES, recs.size(), view -> view.deleteAll(null, keys));

        // Delete non-existing keys.
        testRecordViewOperation(TABLE_NAME, WRITES, 0L, view -> view.deleteAll(null, keys));

        recordView(TABLE_NAME, 0).insert(null, recs.get(0));

        // Delete one non-existing key.
        testRecordViewOperation(TABLE_NAME, WRITES, 1L, view -> view.deleteAll(null, keys));

        // Non-unique keys.
        List<Tuple> nonUniqueKeys = of(
                Tuple.create().set("id", 12),
                Tuple.create().set("id", 42),
                Tuple.create().set("id", 12));
        List<Tuple> nonUniqueRecs = nonUniqueKeys
                .stream()
                .map(t -> Tuple.copy(t).set("val", "value_" + t.intValue("id")))
                .collect(toList());

        recordView(TABLE_NAME, 0).upsertAll(null, nonUniqueRecs);

        testRecordViewOperation(TABLE_NAME, WRITES, 2L, view -> view.deleteAll(null, nonUniqueKeys));
    }

    @Test
    void deleteAllExact() {
        List<Tuple> keys = of(Tuple.create().set("id", 12), Tuple.create().set("id", 42));
        List<Tuple> recs = keys.stream().map(t -> Tuple.copy(t).set("val", "value_" + t.intValue("id"))).collect(toList());

        recordView(TABLE_NAME, 0).upsertAll(null, recs);

        // Delete existing keys.
        testRecordViewOperation(
                TABLE_NAME,
                of(RW_READS, WRITES), of((long) recs.size(), (long) recs.size()),
                view -> view.deleteAllExact(null, recs));

        // Delete non-existing keys.
        testRecordViewOperation(TABLE_NAME, of(RW_READS, WRITES), of((long) recs.size(), 0L), view -> view.deleteAllExact(null, recs));

        recordView(TABLE_NAME, 0).insert(null, recs.get(0));

        // Delete one non-existing key.
        testRecordViewOperation(TABLE_NAME, of(RW_READS, WRITES), of((long) recs.size(), 1L), view -> view.deleteAllExact(null, recs));

        recordView(TABLE_NAME, 0).upsertAll(null, recs);
        List<Tuple> nonExact = keys.stream().map(t -> Tuple.copy(t).set("val", "value_xyz_" + t.intValue("id"))).collect(toList());

        testRecordViewOperation(TABLE_NAME, of(RW_READS, WRITES), of((long) recs.size(), 0L), view -> view.deleteAllExact(null, nonExact));

        // Non-unique keys.
        List<Tuple> nonUniqueKeys = of(
                Tuple.create().set("id", 12),
                Tuple.create().set("id", 42),
                Tuple.create().set("id", 12));
        List<Tuple> nonUniqueRecs = nonUniqueKeys
                .stream()
                .map(t -> Tuple.copy(t).set("val", "value_" + t.intValue("id")))
                .collect(toList());

        recordView(TABLE_NAME, 0).upsertAll(null, nonUniqueRecs);

        testRecordViewOperation(
                TABLE_NAME,
                of(RW_READS, WRITES),
                of((long) nonUniqueRecs.size(), 2L),
                view -> view.deleteAllExact(null, nonUniqueRecs));
    }

    @Test
    void scan() {
        Map<Integer, String> values = Map.of(12, "12", 15, "15", 17, "17", 19, "19", 23, "23");

        KeyValueView<Integer, String> kvView = keyValueView(TABLE_NAME, 0);
        kvView.removeAll(null);
        kvView.putAll(null, values);

        testKeyValueViewOperation(TABLE_NAME, of(RO_READS, RW_READS), of(0L, (long) values.size()), view -> {
            Transaction tx = node(0).transactions().begin();

            Object[] emptyArgs = new Object[0];
            sql(0, tx, "select * from " + TABLE_NAME, emptyArgs);

            tx.commit();
        });

        testKeyValueViewOperation(TABLE_NAME, of(RO_READS, RW_READS), of((long) values.size(), 0L), view -> {
            Object[] emptyArgs = new Object[0];
            sql(0, null, "select * from " + TABLE_NAME, emptyArgs);
        });
    }

    @Test
    void sortedIndexScan() {
        Map<Integer, String> values = Map.of(12, "12", 15, "15", 17, "17", 19, "19", 23, "23");

        KeyValueView<Integer, String> kvView = keyValueView(TABLE_NAME, 0);
        kvView.removeAll(null);
        kvView.putAll(null, values);

        testKeyValueViewOperation(TABLE_NAME, of(RO_READS, RW_READS), of(2L, 0L), view -> {
            Object[] emptyArgs = new Object[0];
            sql(0, null, "select /*+ force_index (" + SORTED_IDX + ") */ * from " + TABLE_NAME + " where id > 15 and id < 20", emptyArgs);
        });

        testKeyValueViewOperation(TABLE_NAME, of(RO_READS, RW_READS), of(0L, 2L), view -> {
            Transaction tx = node(0).transactions().begin();

            Object[] emptyArgs = new Object[0];
            sql(0, tx, "select /*+ force_index (" + SORTED_IDX + ") */ * from " + TABLE_NAME + " where id > 15 and id < 20", emptyArgs);

            tx.commit();
        });
    }

    @Test
    void hashIndexScan() {
        Map<Integer, String> values = Map.of(12, "12", 15, "15", 17, "17", 19, "19", 23, "23");

        KeyValueView<Integer, String> kvView = keyValueView(TABLE_NAME, 0);
        kvView.removeAll(null);
        kvView.putAll(null, values);

        testKeyValueViewOperation(TABLE_NAME, of(RO_READS, RW_READS), of(1L, 0L), view -> {
            Object[] emptyArgs = new Object[0];
            sql(0, null, "select /*+ force_index (" + HASH_IDX + ") */ * from " + TABLE_NAME + " where val = '19'", emptyArgs);
        });

        testKeyValueViewOperation(TABLE_NAME, of(RO_READS, RW_READS), of(0L, 1L), view -> {
            Transaction tx = node(0).transactions().begin();

            Object[] emptyArgs = new Object[0];
            sql(0, tx, "select /*+ force_index (" + HASH_IDX + ") */ * from " + TABLE_NAME + " where val = '19'", emptyArgs);

            tx.commit();
        });
    }

    /**
     * Tests that table metrics are not affected by table renaming.
     */
    @Test
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-19484")
    void tableRename() {
        String initialTableName = "TABLE_TO_RENAME";
        String newTableName = initialTableName + "_RENAMED";

        sql("CREATE TABLE " + initialTableName + " (id INT PRIMARY KEY, val VARCHAR)");

        testKeyValueViewOperation(initialTableName, WRITES, 1L, view -> view.put(null, 42, "value_42"));

        Map<String, Long> initialValues = metricValues(initialTableName, of(RO_READS, RW_READS, WRITES));

        sql("alter table " + initialTableName + " rename to " + newTableName);

        Map<String, Long> newValues = metricValues(newTableName, of(RO_READS, RW_READS, WRITES));

        assertThat(newValues, equalTo(initialValues));
    }

    /**
     * Tests that the given operation increases the specified metric by the expected value.
     *
     * @param tableName Table name.
     * @param metricName Metric name to be checked.
     * @param expectedValue Expected value to increase the metric.
     * @param op Operation to be executed.
     */
    private void testKeyValueViewOperation(
            String tableName,
            String metricName,
            long expectedValue,
            Consumer<KeyValueView<Integer, String>> op
    ) {
        testKeyValueViewOperation(tableName, of(metricName), of(expectedValue), op);
    }

    /**
     * Tests that the given operation increases the specified metrics by the expected values.
     *
     * @param tableName Table name.
     * @param metricNames Metric names to be checked.
     * @param expectedValues Expected values to increase the metrics.
     * @param op Operation to be executed.
     */
    private void testKeyValueViewOperation(
            String tableName,
            List<String> metricNames,
            List<Long> expectedValues,
            Consumer<KeyValueView<Integer, String>> op
    ) {
        testOperation(tableName, metricNames, expectedValues, () -> op.accept(keyValueView(tableName, 0)));
    }

    /**
     * Tests that the given operation increases the specified metric by the expected value.
     *
     * @param tableName Table name.
     * @param metricName Metric name to be checked.
     * @param expectedValue Expected value to increase the metric.
     * @param op Operation to be executed.
     */
    private void testRecordViewOperation(String tableName, String metricName, long expectedValue, Consumer<RecordView<Tuple>> op) {
        testRecordViewOperation(tableName, of(metricName), of(expectedValue), op);
    }

    /**
     * Tests that the given operation increases the specified metrics by the expected values.
     *
     * @param tableName Table name.
     * @param metricNames Metric names to be checked.
     * @param expectedValues Expected values to increase the metrics.
     * @param op Operation to be executed.
     */
    private void testRecordViewOperation(
            String tableName,
            List<String> metricNames,
            List<Long> expectedValues,
            Consumer<RecordView<Tuple>> op
    ) {
        testOperation(tableName, metricNames, expectedValues, () -> op.accept(recordView(tableName, 0)));
    }

    /**
     * Tests that the given operation increases the specified metrics by the expected values.
     *
     * @param tableName Table name.
     * @param metricNames Metric names to be checked.
     * @param expectedValues Expected values to increase the metrics.
     * @param op Operation to be executed.
     */
    private void testOperation(
            String tableName,
            List<String> metricNames,
            List<Long> expectedValues,
            Runnable op
    ) {
        assertThat(metricNames.size(), is(expectedValues.size()));

        Map<String, Long> initialValues = metricValues(tableName, metricNames);

        op.run();

        Map<String, Long> actualValues = metricValues(tableName, metricNames);

        for (int i = 0; i < metricNames.size(); ++i) {
            String metricName = metricNames.get(i);
            long expectedValue = expectedValues.get(i);

            long initialValue = initialValues.get(metricName);
            long actualValue = actualValues.get(metricName);

            assertThat(
                    "The actual metric value does not match the expected value "
                            + "[metric=" + metricName + ", initial=" + initialValue + ", actual=" + actualValue
                            + ", expected=" + (initialValue + expectedValue) + ']',
                    actualValue,
                    is(initialValue + expectedValue));
        }
    }

    /**
     * Returns the sum of the specified metrics on all nodes.
     *
     * @param tableName Table name.
     * @param metricNames Metric names.
     * @return Map of metric names to their values.
     */
    private Map<String, Long> metricValues(String tableName, List<String> metricNames) {
        Map<String, Long> values = new HashMap<>(metricNames.size());

        for (int i = 0; i < initialNodes(); ++i) {
            MetricSet tableMetrics = unwrapIgniteImpl(node(i))
                    .metricManager()
                    .metricSnapshot()
                    .metrics()
                    .get(TableMetricSource.sourceName(QualifiedName.fromSimple(tableName)));

            metricNames.forEach(metricName ->
                    values.compute(metricName, (k, v) -> {
                        Metric metric = tableMetrics.get(metricName);

                        assertThat("Metric not found [name=" + metricName + ']', metric, is(notNullValue()));
                        assertThat(
                                "Metric is not a LongMetric [name=" + metricName + ", class=" + metric.getClass().getSimpleName() + ']',
                                metric,
                                instanceOf(LongMetric.class));

                        return (v == null ? 0 : v) + ((LongMetric) metric).value();
                    }));
        }

        return values;
    }
}
