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

package org.apache.ignite.migrationtools.persistence;

import static org.apache.ignite.migrationtools.sql.SqlDdlGenerator.EXTRA_FIELDS_COLUMN_NAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import org.apache.ignite.binary.BinaryType;
import org.apache.ignite.internal.binary.BinaryObjectImpl;
import org.apache.ignite.migrationtools.persistence.mappers.IgnoreMismatchesSchemaColumnProcessor;
import org.apache.ignite.migrationtools.persistence.mappers.RecordAndTableSchemaMismatchException;
import org.apache.ignite.migrationtools.persistence.mappers.SchemaColumnsProcessor;
import org.apache.ignite.migrationtools.persistence.mappers.SimpleSchemaColumnsProcessor;
import org.apache.ignite.migrationtools.persistence.mappers.SkipRecordsSchemaColumnsProcessor;
import org.apache.ignite.migrationtools.persistence.utils.pubsub.StreamerPublisher;
import org.apache.ignite.migrationtools.types.converters.StaticTypeConverterFactory;
import org.apache.ignite.migrationtools.types.converters.TypeConverterFactory;
import org.apache.ignite3.internal.client.table.ClientColumn;
import org.apache.ignite3.internal.client.table.ClientSchema;
import org.apache.ignite3.sql.ColumnType;
import org.apache.ignite3.table.DataStreamerItem;
import org.apache.ignite3.table.Tuple;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/** SchemaColumnsProcessorTest. */
public class SchemaColumnsProcessorTest {
    // TODO: This test must be totally refactored its impossible to add new functionality easily
    // TODO: Check if I can improve the AssertJ stuff

    private static void assertErrorThrownWithMissingColumn(MyTestSubscriber subscriber, String missingColName) {
        var err = subscriber.error;
        assertThat(err).isNotNull().isInstanceOf(RecordAndTableSchemaMismatchException.class);
        RecordAndTableSchemaMismatchException castErr = (RecordAndTableSchemaMismatchException) err;
        assertThat(castErr.missingColumnsInRecord()).containsOnly(missingColName);
        assertThat(castErr.additionalColumnsInRecord()).isEmpty();
    }

    private static void assertErrorThrownWithAdditionalColumn(MyTestSubscriber subscriber, String additionalColName) {
        var err = subscriber.error;
        assertThat(err).isNotNull().isInstanceOf(RecordAndTableSchemaMismatchException.class);
        RecordAndTableSchemaMismatchException castErr = (RecordAndTableSchemaMismatchException) err;
        assertThat(castErr.additionalColumnsInRecord()).containsOnly(additionalColName);
        assertThat(castErr.missingColumnsInRecord()).isEmpty();
    }

    private static BinaryType createBinaryType(int id, List<String> fieldNames) {
        var bt = Mockito.mock(BinaryType.class);
        Mockito.doReturn(id).when(bt).typeId();
        Mockito.doReturn(fieldNames).when(bt).fieldNames();
        return bt;
    }

    private static BinaryObjectImpl createBinaryObject(Map<String, Object> obj) {
        var bt = createBinaryType(100, new ArrayList<>(obj.keySet()));
        return createBinaryObject(bt, obj);
    }

    private static BinaryObjectImpl createBinaryObject(BinaryType bt, Map<String, Object> obj) {
        var m = Mockito.mock(BinaryObjectImpl.class);
        Mockito.doReturn(bt).when(m).rawType();
        Mockito.doAnswer(ans -> {
            String fieldName = ans.getArgument(0);
            assertThat(obj).containsKey(fieldName);
            return obj.get(fieldName);
        }).when(m).field(Mockito.any(String.class));

        return m;
    }

    private static void assertColumnDoesNotExistsOrIsNull(Tuple t, String colName) {
        var colIdx = t.columnIndex(colName);

        if (colIdx != -1) {
            var v = t.value("VAL_1");
            assertThat(v).isNull();
            assertThat(v).isEqualTo(t.value(colIdx));
        }
    }

    private abstract static class TestCases {

        protected abstract SchemaColumnsProcessor createSchemaColumnProcessor(
                ClientSchema schema,
                Map<String, String> fieldForColumnName,
                TypeConverterFactory defaultConverters);

        @Test
        void testWithValidPojoValue() throws InterruptedException {
            ClientColumn[] cols = new ClientColumn[] {
                    new ClientColumn("ID", ColumnType.INT32, false, 0, -1, 0, 0, 0, 0),
                    new ClientColumn("VAL_0", ColumnType.INT32, false, -1, 0, 1, 1, 0, 0),
                    new ClientColumn("VAL_1", ColumnType.INT32, false, -1, 1, 2, 2, 0, 0)
            };

            List<Map.Entry<Object, Object>> tuples = List.of(Map.entry(1, createBinaryObject(Map.of("VAL_0", 100, "VAL_1", 200))));
            var subscriber = submitRecords(cols, tuples, Collections.emptyMap());

            assertEquals(subscriber.key.columnCount(), 1);
            assertEquals(subscriber.key.columnName(0), "ID");

            assertEquals(subscriber.val.columnCount(), 2);
            assertEquals("VAL_0", subscriber.val.columnName(0));
            assertEquals("VAL_1", subscriber.val.columnName(1));
            assertEquals(Integer.valueOf(100), subscriber.val.value("VAL_0"));
            assertEquals(Integer.valueOf(200), subscriber.val.value("VAL_1"));
        }

        // If both table and record have just 1 column -> Rename tuple to table column name
        // Possible Errors
        // Additional nullable column in table -> Add null to record.
        // Additional not nullable column in table -> Throw exception.
        // Additional column in record -> Throw exception can lead to data loss.

        @Test
        void testWithMissingNullableColumnInRecord() throws InterruptedException {
            // Should add null field to the record.
            ClientColumn[] cols = new ClientColumn[] {
                    new ClientColumn("ID", ColumnType.INT32, false, 0, -1, 0, 0, 0, 0),
                    new ClientColumn("VAL_0", ColumnType.INT32, false, -1, 0, 1, 1, 0, 0),
                    new ClientColumn("VAL_1", ColumnType.INT32, true, -1, 1, 2, 2, 0, 0)
            };

            List<Map.Entry<Object, Object>> tuples = List.of(Map.entry(1, createBinaryObject(Map.of("VAL_0", 100))));
            var subscriber = submitRecords(cols, tuples, Collections.emptyMap());

            assertThat(subscriber.error).isNull();
            assertThat(subscriber.val).isNotNull();

            assertThat(subscriber.val.columnCount()).isIn(1, 2);
            assertColumnDoesNotExistsOrIsNull(subscriber.val, "VAL_1");
        }

        @Test
        void testWithMissingNotNullableColumnInRecord() throws InterruptedException {
            // Should throw exception.
            ClientColumn[] cols = new ClientColumn[] {
                    new ClientColumn("ID", ColumnType.INT32, false, 0, -1, 0, 0, 0, 0),
                    new ClientColumn("VAL_0", ColumnType.INT32, false, -1, 0, 1, 1, 0, 0),
                    new ClientColumn("VAL_1", ColumnType.INT32, false, -1, 1, 2, 2, 0, 0)
            };

            Map<String, Object> record1 = Map.of("VAL_0", 100);
            Map<String, Object> record2 = Map.of("VAL_0", 200, "VAL_1", 201);

            BinaryType keyType = createBinaryType(101, new ArrayList<>(record1.keySet()));
            BinaryType valType = createBinaryType(102, new ArrayList<>(record2.keySet()));

            List<Map.Entry<Object, Object>> data = List.of(
                    Map.entry(1, createBinaryObject(keyType, record1)),
                    Map.entry(2, createBinaryObject(valType, record2))
            );

            var subscriber = submitRecords(cols, data, Collections.emptyMap());

            whenMissingNotNullableColumnInRecord(subscriber, "VAL_1");
        }

        abstract void whenMissingNotNullableColumnInRecord(MyTestSubscriber subscriber, String missingColName);

        @Test
        void testWithAdditionalColumnInRecordWithoutExtra() throws InterruptedException {
            // Should throw exception.
            ClientColumn[] cols = new ClientColumn[] {
                    new ClientColumn("ID", ColumnType.INT32, false, 0, -1, 0, 0, 0, 0),
                    new ClientColumn("VAL_0", ColumnType.INT32, false, -1, 0, 1, 1, 0, 0)
            };

            Map<String, Object> record1 = Map.of("VAL_0", 100, "VAL_1", 101);
            Map<String, Object> record2 = Map.of("VAL_0", 200);

            BinaryType keyType = createBinaryType(101, new ArrayList<>(record1.keySet()));
            BinaryType valType = createBinaryType(102, new ArrayList<>(record2.keySet()));

            List<Map.Entry<Object, Object>> data = List.of(
                    Map.entry(1, createBinaryObject(keyType, record1)),
                    Map.entry(2, createBinaryObject(valType, record2))
            );

            var subscriber = submitRecords(cols, data, Collections.emptyMap());

            whenAdditionalColumnInRecord(subscriber, "VAL_1", false);
        }

        @Test
        void testWithAdditionalColumnInRecordWithExtra() throws InterruptedException {
            // Should throw exception.
            ClientColumn[] cols = new ClientColumn[] {
                    new ClientColumn("ID", ColumnType.INT32, false, 0, -1, 0, 0, 0, 0),
                    new ClientColumn("VAL_0", ColumnType.INT32, false, -1, 0, 1, 1, 0, 0),
                    new ClientColumn(EXTRA_FIELDS_COLUMN_NAME, ColumnType.BYTE_ARRAY, true, -1, 1, 2, 1, 0, 0)
            };

            Map<String, Object> record1 = Map.of("VAL_0", 100, "VAL_1", 101);
            Map<String, Object> record2 = Map.of("VAL_0", 200);

            BinaryType keyType = createBinaryType(101, new ArrayList<>(record1.keySet()));
            BinaryType valType = createBinaryType(102, new ArrayList<>(record2.keySet()));

            List<Map.Entry<Object, Object>> data = List.of(
                    Map.entry(1, createBinaryObject(keyType, record1)),
                    Map.entry(2, createBinaryObject(valType, record2))
            );

            var subscriber = submitRecords(cols, data, Collections.emptyMap());

            whenAdditionalColumnInRecord(subscriber, "VAL_1", true);
        }

        abstract void whenAdditionalColumnInRecord(MyTestSubscriber subscriber, String additionalColName, boolean hasExtraColumn);

        @Test
        void testWithOverlapingFieldsOnKeyAndValueTuple() throws InterruptedException {
            ClientColumn[] cols = new ClientColumn[] {
                    new ClientColumn("FIRST_ID", ColumnType.INT32, false, 0, -1, 0, 0, 0, 0),
                    new ClientColumn("SCND_KEY", ColumnType.INT32, false, 1, -1, 1, 1, 0, 0),
                    new ClientColumn("VAL_0", ColumnType.INT32, false, -1, 0, 2, 2, 0, 0),
                    new ClientColumn("VAL_1", ColumnType.INT32, false, -1, 1, 3, 3, 0, 0)
            };

            var keyTuple = Tuple.create(2).set("FIRST_ID", 1).set("SCND_KEY", 2);
            var valTuple = Tuple.copy(keyTuple).set("VAL_0", 100).set("VAL_1", 200);

            var tuples = List.of(Map.entry(keyTuple, valTuple));
            var subscriber = submitRecords(cols, tuples);

            var expectedKeys = Tuple.copy(keyTuple);
            assertThat(subscriber.key).isEqualTo(expectedKeys);

            var expectedValues = Tuple.create(2).set("VAL_0", 100).set("VAL_1", 200);
            assertThat(subscriber.val).isEqualTo(expectedValues);
        }

        @Test
        void testWithAliases() throws InterruptedException {
            ClientColumn[] cols = new ClientColumn[] {
                    new ClientColumn("FIRST_ID", ColumnType.INT32, false, 0, -1, 0, 0, 0, 0),
                    new ClientColumn("SCND_KEY", ColumnType.INT32, false, 1, -1, 1, 1, 0, 0),
                    new ClientColumn("VAL_0", ColumnType.INT32, false, -1, 0, 2, 2, 0, 0),
                    new ClientColumn("VAL_1", ColumnType.INT32, false, -1, 1, 3, 3, 0, 0)
            };

            Map<String, Object> keyFields = Map.of("FIRST_ID", 1, "SCND_KEY", 2);
            Map<String, Object> valFields = Map.of("FIRST_ID", 1, "SCND_KEY", 2, "SOME_ATTRIBUTE", 100, "ANOTHER_ATTR", 200);

            var keyTuple = createBinaryObject(keyFields);
            var valTuple = createBinaryObject(valFields);

            List<Map.Entry<Object, Object>> tuples = List.of(Map.entry(keyTuple, valTuple));
            var fieldAliases = Map.of("VAL_1", "ANOTHER_ATTR", "VAL_0", "SOME_ATTRIBUTE");
            var subscriber = submitRecords(cols, tuples, fieldAliases);

            var expectedKeys = Tuple.create(keyFields);
            assertThat(subscriber.key).isEqualTo(expectedKeys);

            var expectedValues = Tuple.create(2).set("VAL_0", 100).set("VAL_1", 200);
            assertThat(subscriber.val).isEqualTo(expectedValues);
        }

        private MyTestSubscriber submitRecords(ClientColumn[] cols, List<Map.Entry<Tuple, Tuple>> records) throws InterruptedException {
            List<Map.Entry<Object, Object>> nr = records.stream()
                    .map(e -> {
                        var k = e.getKey();
                        var v = e.getValue();

                        Map<String, Object> keyData = new HashMap<>();
                        for (int i = 0; i < k.columnCount(); i++) {
                            keyData.put(k.columnName(i), k.value(i));
                        }

                        Map<String, Object> valueData = new HashMap<>();
                        for (int i = 0; i < v.columnCount(); i++) {
                            valueData.put(v.columnName(i), v.value(i));
                        }

                        return Map.entry((Object) createBinaryObject(keyData), (Object) createBinaryObject(valueData));
                    }).collect(Collectors.toList());

            return submitRecords(cols, nr, Collections.emptyMap());
        }

        private MyTestSubscriber submitRecords(
                ClientColumn[] cols,
                List<Map.Entry<Object, Object>> records,
                Map<String, String> fieldForColumnName
        ) throws InterruptedException {
            var schema = new ClientSchema(0, cols, null);

            var subscriber = new MyTestSubscriber();
            try (StreamerPublisher<Map.Entry<Object, Object>> base = new StreamerPublisher<>()) {
                var columnProcessor = createSchemaColumnProcessor(schema, fieldForColumnName, StaticTypeConverterFactory.DEFAULT_INSTANCE);
                base.subscribe(columnProcessor);
                columnProcessor.subscribe(subscriber);
                for (var record : records) {
                    assertThat(base.offer(record)).isTrue();
                }
            }

            subscriber.latch.await();

            return subscriber;
        }
    }

    private static class MyTestSubscriber implements Flow.Subscriber<DataStreamerItem<Map.Entry<Tuple, Tuple>>> {

        private final CountDownLatch latch = new CountDownLatch(1);

        private AtomicBoolean setFlag = new AtomicBoolean(false);

        private Tuple key;

        private Tuple val;

        private Throwable error;

        @Override
        public void onSubscribe(Flow.Subscription subscription) {
            subscription.request(100);
        }

        @Override
        public void onNext(DataStreamerItem<Map.Entry<Tuple, Tuple>> dsItem) {
            if (setFlag.compareAndSet(false, true)) {
                var item = dsItem.get();
                this.key = item.getKey();
                this.val = item.getValue();
            }
        }

        @Override
        public void onError(Throwable throwable) {
            this.error = throwable;
            latch.countDown();
        }

        @Override
        public void onComplete() {
            latch.countDown();
        }
    }

    @Nested
    class TestSimpleSchemaColumnsProcessor extends TestCases {
        @Override
        protected SchemaColumnsProcessor createSchemaColumnProcessor(ClientSchema schema, Map<String, String> fieldForColumnName,
                TypeConverterFactory defaultConverters) {
            return new SimpleSchemaColumnsProcessor(schema, fieldForColumnName, defaultConverters);
        }

        @Override
        void whenMissingNotNullableColumnInRecord(MyTestSubscriber subscriber, String missingColName) {
            assertErrorThrownWithMissingColumn(subscriber, missingColName);
        }

        @Override
        void whenAdditionalColumnInRecord(MyTestSubscriber subscriber, String additionalColName, boolean hasExtraColumn) {
            assertErrorThrownWithAdditionalColumn(subscriber, additionalColName);
        }
    }

    @Nested
    class TestIgnoreMismatchedColumnsProcessor extends TestCases {
        @Override
        protected SchemaColumnsProcessor createSchemaColumnProcessor(ClientSchema schema, Map<String, String> fieldForColumnName,
                TypeConverterFactory defaultConverters) {
            return new IgnoreMismatchesSchemaColumnProcessor(schema, fieldForColumnName, defaultConverters);
        }

        @Override
        void whenMissingNotNullableColumnInRecord(MyTestSubscriber subscriber, String missingColName) {
            // Check if we received the error on the first instance.
            assertErrorThrownWithMissingColumn(subscriber, missingColName);
        }

        @Override
        void whenAdditionalColumnInRecord(MyTestSubscriber subscriber, String additionalColName, boolean hasExtraColumn) {
            // Check if we received the first value without the additionalColumn
            assertThat(subscriber.error).isNull();
            assertThat(subscriber.val).isNotNull();
            assertThat(subscriber.val.columnIndex(additionalColName)).isEqualTo(-1);

            if (!hasExtraColumn) {
                assertThat(subscriber.val.columnCount()).isEqualTo(1);
            } else {
                assertColumnDoesNotExistsOrIsNull(subscriber.val, EXTRA_FIELDS_COLUMN_NAME);
            }

            assertThat(subscriber.val.intValue("VAL_0")).isEqualTo(100);
        }
    }

    @Nested
    class TestSkipMismatchedRecordsProcessor extends TestCases {
        @Override
        protected SchemaColumnsProcessor createSchemaColumnProcessor(ClientSchema schema, Map<String, String> fieldForColumnName,
                TypeConverterFactory defaultConverters) {
            return new SkipRecordsSchemaColumnsProcessor(schema, fieldForColumnName, defaultConverters);
        }

        @Override
        void whenMissingNotNullableColumnInRecord(MyTestSubscriber subscriber, String missingColName) {
            // We should not receive the first value. But receive the second.
            // Check if we received the second value
            assertThat(subscriber.error).isNull();
            assertThat(subscriber.val).isNotNull();
            assertThat(subscriber.val.columnCount()).isEqualTo(2);
            assertThat(subscriber.val.intValue("VAL_0")).isEqualTo(200);
            assertThat(subscriber.val.intValue("VAL_1")).isEqualTo(201);
        }

        @Override
        void whenAdditionalColumnInRecord(MyTestSubscriber subscriber, String additionalColName, boolean hasExtraColumn) {
            // We should not receive the first value. But receive the second.
            // Check if we received the second value
            assertThat(subscriber.error).isNull();
            assertThat(subscriber.val).isNotNull();

            if (!hasExtraColumn) {
                assertThat(subscriber.val.columnCount()).isEqualTo(1);
            } else {
                assertColumnDoesNotExistsOrIsNull(subscriber.val, EXTRA_FIELDS_COLUMN_NAME);
            }

            assertThat(subscriber.val.intValue("VAL_0")).isEqualTo(200);
        }
    }

    @Nested
    class TestExtraFieldsColumnsProcessor extends TestCases {
        @Override
        protected SchemaColumnsProcessor createSchemaColumnProcessor(ClientSchema schema, Map<String, String> fieldForColumnName,
                TypeConverterFactory defaultConverters) {
            return new SimpleSchemaColumnsProcessor(schema, fieldForColumnName, defaultConverters, true);
        }

        @Override
        void whenMissingNotNullableColumnInRecord(MyTestSubscriber subscriber, String missingColName) {
            // Check if we received the error on the first instance.
            assertErrorThrownWithMissingColumn(subscriber, missingColName);
        }

        @Override
        void whenAdditionalColumnInRecord(MyTestSubscriber subscriber, String additionalColName, boolean hasExtraColumn) {
            if (hasExtraColumn) {
                assertThat(subscriber.error).isNull();
                assertThat(subscriber.val).isNotNull();

                int extraColumnIdx = subscriber.val.columnIndex(EXTRA_FIELDS_COLUMN_NAME);
                byte[] value = subscriber.val.value(extraColumnIdx);

                assertThat(value).isNotEmpty();
            } else {
                assertErrorThrownWithAdditionalColumn(subscriber, additionalColName);
            }
        }
    }
}
