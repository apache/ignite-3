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

package org.apache.ignite.internal.table.distributed.replicator;

import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.DEFAULT_LENGTH;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.DEFAULT_PRECISION;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.DEFAULT_SCALE;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.sql.ColumnType.BITMASK;
import static org.apache.ignite.sql.ColumnType.BOOLEAN;
import static org.apache.ignite.sql.ColumnType.BYTE_ARRAY;
import static org.apache.ignite.sql.ColumnType.DECIMAL;
import static org.apache.ignite.sql.ColumnType.DOUBLE;
import static org.apache.ignite.sql.ColumnType.DURATION;
import static org.apache.ignite.sql.ColumnType.FLOAT;
import static org.apache.ignite.sql.ColumnType.INT16;
import static org.apache.ignite.sql.ColumnType.INT32;
import static org.apache.ignite.sql.ColumnType.INT64;
import static org.apache.ignite.sql.ColumnType.INT8;
import static org.apache.ignite.sql.ColumnType.NUMBER;
import static org.apache.ignite.sql.ColumnType.PERIOD;
import static org.apache.ignite.sql.ColumnType.STRING;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.catalog.commands.DefaultValue;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableColumnDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.IgniteBiTuple;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.table.distributed.schema.AlwaysSyncedSchemaSyncService;
import org.apache.ignite.internal.table.distributed.schema.FullTableSchema;
import org.apache.ignite.internal.table.distributed.schema.ValidationSchemasSource;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.tx.TransactionIds;
import org.apache.ignite.sql.ColumnType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SchemaCompatibilityValidatorTest extends BaseIgniteAbstractTest {
    @Mock
    private ValidationSchemasSource schemasSource;

    @Mock
    private CatalogService catalogService;

    private SchemaCompatibilityValidator validator;

    private final HybridTimestamp beginTimestamp = new HybridTimestamp(1, 1);

    private final HybridTimestamp commitTimestamp = new HybridTimestamp(2, 2);

    private final UUID txId = TransactionIds.transactionId(beginTimestamp, 0);

    private static final int TABLE_ID = 1;
    private static final String TABLE_NAME = "test";
    private static final String ANOTHER_NAME = "another";

    private final TablePartitionId tablePartitionId = new TablePartitionId(TABLE_ID, 0);

    @BeforeEach
    void createValidatorAndInitMocks() {
        lenient().when(catalogService.table(TABLE_ID, commitTimestamp.longValue())).thenReturn(mock(CatalogTableDescriptor.class));

        validator = new SchemaCompatibilityValidator(schemasSource, catalogService, new AlwaysSyncedSchemaSyncService());
    }

    @ParameterizedTest
    @EnumSource(ForwardCompatibleChange.class)
    void forwardCompatibleChangesAllowCommitting(ForwardCompatibleChange change) {
        assertForwardCompatibleChangeAllowsCommitting(change);
    }

    private void assertForwardCompatibleChangeAllowsCommitting(SchemaChangeSource changeSource) {
        when(schemasSource.tableSchemaVersionsBetween(TABLE_ID, beginTimestamp, commitTimestamp))
                .thenReturn(changeSource.schemaVersions());

        CompletableFuture<CompatValidationResult> resultFuture = validator.validateCommit(txId, List.of(tablePartitionId), commitTimestamp);

        assertThat(resultFuture, willCompleteSuccessfully());

        CompatValidationResult result = resultFuture.getNow(null);
        assertThat(result, is(notNullValue()));

        assertThat("Change is incompatible", result.isSuccessful(), is(true));
    }

    @ParameterizedTest
    @EnumSource(ForwardIncompatibleChange.class)
    void forwardIncompatibleChangesDisallowCommitting(ForwardIncompatibleChange change) {
        assertForwardIncompatibleChangeDisallowsCommitting(change);
    }

    private void assertForwardIncompatibleChangeDisallowsCommitting(SchemaChangeSource changeSource) {
        when(schemasSource.tableSchemaVersionsBetween(TABLE_ID, beginTimestamp, commitTimestamp))
                .thenReturn(changeSource.schemaVersions());

        CompletableFuture<CompatValidationResult> resultFuture = validator.validateCommit(txId, List.of(tablePartitionId), commitTimestamp);

        assertThat(resultFuture, willCompleteSuccessfully());

        CompatValidationResult result = resultFuture.getNow(null);
        assertThat(result, is(notNullValue()));

        assertThat("Change is compatible", result.isSuccessful(), is(false));
        assertThat(result.isTableDropped(), is(false));

        assertThat(result.failedTableName(), is(TABLE_NAME));
        assertThat(result.fromSchemaVersion(), is(1));
        assertThat(result.toSchemaVersion(), is(2));
    }

    @ParameterizedTest
    @MethodSource("exactColumnTypeChanges")
    void exactColumnTypeChangesAllowCommitting(ColumnTypeChange change) {
        assertForwardCompatibleChangeAllowsCommitting(change);
    }

    private static Stream<Arguments> exactColumnTypeChanges() {
        List<ColumnTypeChange> changes = new ArrayList<>();

        for (IgniteBiTuple<ColumnType, ColumnType> pair : simpleTypeCompatibleChanges()) {
            //noinspection ConstantConditions
            changes.add(new ColumnTypeChange(pair.get1(), pair.get2()));
        }

        // Increasing precision.
        changes.add(new ColumnTypeChange(decimal(10, 5), decimal(11, 5)));

        // Increasing length.
        changes.add(new ColumnTypeChange(string(10), string(20)));
        changes.add(new ColumnTypeChange(byteArray(10), byteArray(20)));

        return changes.stream().map(Arguments::of);
    }

    private static List<IgniteBiTuple<ColumnType, ColumnType>> simpleTypeCompatibleChanges() {
        List<IgniteBiTuple<ColumnType, ColumnType>> changes = new ArrayList<>();

        List<ColumnType> intTypes = List.of(INT8, INT16, INT32, INT64);

        // INT8->INT16->INT32->INT64.
        for (int i = 0; i < intTypes.size() - 1; i++) {
            ColumnType narrowerType = intTypes.get(i);

            for (int j = i + 1; j < intTypes.size(); j++) {
                ColumnType widerType = intTypes.get(j);
                changes.add(new IgniteBiTuple<>(narrowerType, widerType));
            }
        }

        changes.add(new IgniteBiTuple<>(FLOAT, DOUBLE));

        return List.copyOf(changes);
    }

    private static Type decimal(int precision, int scale) {
        return new Type(DECIMAL, precision, scale, DEFAULT_LENGTH);
    }

    private static Type number(int precision) {
        return new Type(NUMBER, precision, DEFAULT_SCALE, DEFAULT_LENGTH);
    }

    private static Type string(int length) {
        return new Type(STRING, DEFAULT_PRECISION, DEFAULT_SCALE, length);
    }

    private static Type byteArray(int length) {
        return new Type(BYTE_ARRAY, DEFAULT_PRECISION, DEFAULT_SCALE, length);
    }

    @ParameterizedTest
    @MethodSource("nonExactColumnTypeChanges")
    void nonExactColumnTypeChangesDisallowCommitting(ColumnTypeChange change) {
        assertForwardIncompatibleChangeDisallowsCommitting(change);
    }

    private static Stream<Arguments> nonExactColumnTypeChanges() {
        List<ColumnTypeChange> changes = new ArrayList<>();

        List<ColumnType> simpleTypes = Arrays.stream(ColumnType.values())
                .filter(type -> !type.precisionAllowed() && !type.scaleAllowed() && !type.lengthAllowed())
                .collect(toList());

        List<IgniteBiTuple<ColumnType, ColumnType>> simpleTypeCompatibleChanges = simpleTypeCompatibleChanges();
        Map<ColumnType, Set<ColumnType>> typeToTypes = simpleTypeCompatibleChanges.stream()
                .collect(groupingBy(IgniteBiTuple::get1, Collectors.mapping(IgniteBiTuple::get2, toSet())));

        for (ColumnType type1 : simpleTypes) {
            for (ColumnType type2 : simpleTypes) {
                if (type1 == type2) {
                    continue;
                }
                Set<ColumnType> types = typeToTypes.get(type1);
                if (types == null || !types.contains(type2)) {
                    changes.add(new ColumnTypeChange(type1, type2));
                }
            }
        }

        changes.add(new ColumnTypeChange(INT8, number(100)));
        changes.add(new ColumnTypeChange(INT16, number(100)));
        changes.add(new ColumnTypeChange(INT32, number(100)));
        changes.add(new ColumnTypeChange(INT64, number(100)));
        changes.add(new ColumnTypeChange(INT8, decimal(100, 0)));
        changes.add(new ColumnTypeChange(INT16, decimal(100, 0)));
        changes.add(new ColumnTypeChange(INT32, decimal(100, 0)));
        changes.add(new ColumnTypeChange(INT64, decimal(100, 0)));

        changes.add(new ColumnTypeChange(number(10), decimal(100, 0)));

        // Decreasing precision.
        changes.add(new ColumnTypeChange(decimal(10, 5), decimal(9, 5)));
        changes.add(new ColumnTypeChange(number(10), number(9)));

        // Decreasing length.
        changes.add(new ColumnTypeChange(string(10), string(9)));
        changes.add(new ColumnTypeChange(byteArray(10), byteArray(9)));

        // Conversions to STRING.
        changes.add(new ColumnTypeChange(INT8, string(100)));
        changes.add(new ColumnTypeChange(INT16, string(100)));
        changes.add(new ColumnTypeChange(INT32, string(100)));
        changes.add(new ColumnTypeChange(INT64, string(100)));
        changes.add(new ColumnTypeChange(decimal(10, 0), string(100)));
        changes.add(new ColumnTypeChange(number(10), string(100)));
        changes.add(new ColumnTypeChange(ColumnType.UUID, string(100)));

        // Conversions from STRING.
        changes.add(new ColumnTypeChange(string(1), INT8));
        changes.add(new ColumnTypeChange(string(1), INT16));
        changes.add(new ColumnTypeChange(string(1), INT32));
        changes.add(new ColumnTypeChange(string(1), INT64));
        changes.add(new ColumnTypeChange(string(1), decimal(10, 0)));
        changes.add(new ColumnTypeChange(string(1), number(10)));
        changes.add(new ColumnTypeChange(string(36), ColumnType.UUID));

        for (ColumnType columnType : ColumnType.values()) {
            addInconvertible(columnType, BOOLEAN, changes);
            addInconvertible(columnType, ColumnType.UUID, changes);
            addInconvertible(columnType, BITMASK, changes);
            addInconvertible(columnType, BYTE_ARRAY, changes);
            addInconvertible(columnType, PERIOD, changes);
            addInconvertible(columnType, DURATION, changes);
        }

        return changes.stream().map(Arguments::of);
    }

    private static void addInconvertible(ColumnType type1, ColumnType type2, List<ColumnTypeChange> changes) {
        if (type1 != type2) {
            changes.add(new ColumnTypeChange(type2, type1));
            changes.add(new ColumnTypeChange(type1, type2));
        }
    }

    @Test
    void combinationOfForwardCompatibleChangesIsCompatible() {
        assertForwardCompatibleChangeAllowsCommitting(() -> List.of(
                tableSchema(1, List.of(column(INT32, false))),
                // Type is widened, NOT NULL dropped.
                tableSchema(2, List.of(column(INT64, true)))
        ));
    }

    private static CatalogTableColumnDescriptor column(ColumnType type, boolean nullable) {
        return new CatalogTableColumnDescriptor(
                "col",
                type,
                nullable,
                DEFAULT_PRECISION,
                DEFAULT_SCALE,
                DEFAULT_LENGTH,
                null
        );
    }

    @Test
    void oneForwardIncompatibleChangeMakesCombinationIncompatible() {
        assertForwardIncompatibleChangeDisallowsCommitting(() -> List.of(
                tableSchema(1, List.of(column(INT32, true))),
                // Type is widened (compatible), but NOT NULL added (incompatible).
                tableSchema(2, List.of(column(INT64, false)))
        ));
    }

    @Test
    void forwardCompatibleChangeIsNotBackwardCompatible() {
        assertChangeIsNotBackwardCompatible(ForwardCompatibleChange.ADD_NULLABLE_COLUMN);
    }

    @Test
    void forwardIncompatibleChangeIsNotBackwardCompatible() {
        assertChangeIsNotBackwardCompatible(ForwardIncompatibleChange.DROP_COLUMN);
    }

    @Test
    void changeOppositeToForwardCompatibleChangeIsNotBackwardCompatible() {
        assertChangeIsNotBackwardCompatible(() -> List.of(
                tableSchema(1, List.of(
                        intColumn("col1"),
                        intColumnWithDefault("col2", 42)
                )),
                tableSchema(2, List.of(
                        intColumn("col1")
                ))
        ));
    }

    private void assertChangeIsNotBackwardCompatible(SchemaChangeSource changeSource) {
        when(schemasSource.tableSchemaVersionsBetween(TABLE_ID, beginTimestamp, 2))
                .thenReturn(changeSource.schemaVersions());
        when(schemasSource.waitForSchemaAvailability(anyInt(), anyInt())).thenReturn(nullCompletedFuture());

        CompletableFuture<CompatValidationResult> resultFuture = validator.validateBackwards(2, TABLE_ID, txId);

        assertThat(resultFuture, willCompleteSuccessfully());

        CompatValidationResult result = resultFuture.getNow(null);
        assertThat(result, is(notNullValue()));

        assertThat("Change is compatible", result.isSuccessful(), is(false));
        assertThat(result.isTableDropped(), is(false));

        assertThat(result.failedTableName(), is(TABLE_NAME));
        assertThat(result.fromSchemaVersion(), is(1));
        assertThat(result.toSchemaVersion(), is(2));
    }

    private static CatalogTableColumnDescriptor intColumn(String columnName) {
        return new CatalogTableColumnDescriptor(
                columnName,
                INT32,
                false,
                DEFAULT_PRECISION,
                DEFAULT_SCALE,
                DEFAULT_LENGTH,
                null
        );
    }

    private static CatalogTableColumnDescriptor nullableIntColumn(String columnName) {
        return new CatalogTableColumnDescriptor(columnName, INT32, true, DEFAULT_PRECISION, DEFAULT_SCALE, DEFAULT_LENGTH, null);
    }

    private static CatalogTableColumnDescriptor intColumnWithDefault(String columnName, int defaultValue) {
        return new CatalogTableColumnDescriptor(
                columnName,
                INT32,
                false,
                DEFAULT_PRECISION,
                DEFAULT_SCALE,
                DEFAULT_LENGTH,
                DefaultValue.constant(defaultValue)
        );
    }

    private static FullTableSchema tableSchema(int schemaVersion, List<CatalogTableColumnDescriptor> columns) {
        return tableSchema(schemaVersion, TABLE_NAME, columns);
    }

    private static FullTableSchema tableSchema(int schemaVersion, String name, List<CatalogTableColumnDescriptor> columns) {
        return new FullTableSchema(schemaVersion, TABLE_ID, name, columns);
    }

    @FunctionalInterface
    private interface SchemaChangeSource {
        List<FullTableSchema> schemaVersions();
    }

    private enum ForwardCompatibleChange implements SchemaChangeSource {
        ADD_NULLABLE_COLUMN(List.of(
                tableSchema(1, List.of(
                        intColumn("col1")
                )),
                tableSchema(2, List.of(
                        intColumn("col1"),
                        nullableIntColumn("col2")
                ))
        )),
        ADD_COLUMN_WITH_DEFAULT(List.of(
                tableSchema(1, List.of(
                        intColumn("col1")
                )),
                tableSchema(2, List.of(
                        intColumn("col1"),
                        intColumnWithDefault("col2", 42)
                ))
        )),
        // TODO: https://issues.apache.org/jira/browse/IGNITE-20948 - uncomment this.
        // RENAME_COLUMN(List.of(
        //        tableSchema(1, List.of(
        //                intColumn("col1")
        //        )),
        //        tableSchema(2, List.of(
        //                intColumn("col2")
        //        ))
        // )),
        DROP_NOT_NULL(List.of(
                tableSchema(1, List.of(
                        intColumn("col1")
                )),
                tableSchema(2, List.of(
                        nullableIntColumn("col1")
                ))
        ));

        private final List<FullTableSchema> schemaVersions;

        ForwardCompatibleChange(List<FullTableSchema> schemaVersions) {
            this.schemaVersions = schemaVersions;
        }

        @Override
        public List<FullTableSchema> schemaVersions() {
            return schemaVersions;
        }
    }

    private enum ForwardIncompatibleChange implements SchemaChangeSource {
        RENAME_TABLE(List.of(
                tableSchema(1, TABLE_NAME, List.of(
                        intColumn("col1")
                )),
                tableSchema(2, ANOTHER_NAME, List.of(
                        intColumn("col1")
                ))
        )),
        DROP_COLUMN(List.of(
                tableSchema(1, List.of(
                        intColumn("col1"),
                        intColumn("col2")
                )),
                tableSchema(2, List.of(
                        intColumn("col1")
                ))
        )),
        ADD_DEFAULT(List.of(
                tableSchema(1, List.of(
                        intColumn("col1")
                )),
                tableSchema(2, List.of(
                        intColumnWithDefault("col1", 42)
                ))
        )),
        CHANGE_DEFAULT(List.of(
                tableSchema(1, List.of(
                        intColumnWithDefault("col1", 1)
                )),
                tableSchema(2, List.of(
                        intColumnWithDefault("col1", 2)
                ))
        )),
        DROP_DEFAULT(List.of(
                tableSchema(1, List.of(
                        intColumnWithDefault("col1", 42)
                )),
                tableSchema(2, List.of(
                        intColumn("col1")
                ))
        ));

        private final List<FullTableSchema> schemaVersions;

        ForwardIncompatibleChange(List<FullTableSchema> schemaVersions) {
            this.schemaVersions = schemaVersions;
        }

        @Override
        public List<FullTableSchema> schemaVersions() {
            return schemaVersions;
        }
    }

    private static class Type {
        private final ColumnType columnType;
        private final int precision;
        private final int scale;
        private final int length;

        private Type(ColumnType columnType) {
            this(columnType, DEFAULT_PRECISION, DEFAULT_SCALE, DEFAULT_LENGTH);
        }

        private Type(ColumnType columnType, int precision, int scale, int length) {
            this.columnType = columnType;
            this.precision = precision;
            this.scale = scale;
            this.length = length;
        }

        @Override
        public String toString() {
            if (precision == DEFAULT_PRECISION && scale == DEFAULT_SCALE && length == DEFAULT_LENGTH) {
                return columnType.toString();
            } else if (precision == DEFAULT_PRECISION && scale == DEFAULT_SCALE) {
                return String.format("%s(%d)", columnType, length);
            } else {
                return String.format("%s(%d/%d/%d)", columnType, precision, scale, length);
            }
        }
    }

    private static class ColumnTypeChange implements SchemaChangeSource {
        private final Type typeBefore;
        private final Type typeAfter;

        private ColumnTypeChange(ColumnType typeBefore, ColumnType typeAfter) {
            this(new Type(typeBefore), new Type(typeAfter));
        }

        private ColumnTypeChange(ColumnType typeBefore, Type typeAfter) {
            this(new Type(typeBefore), typeAfter);
        }

        private ColumnTypeChange(Type typeBefore, ColumnType typeAfter) {
            this(typeBefore, new Type(typeAfter));
        }

        private ColumnTypeChange(Type typeBefore, Type typeAfter) {
            this.typeBefore = typeBefore;
            this.typeAfter = typeAfter;
        }

        @Override
        public List<FullTableSchema> schemaVersions() {
            return List.of(
                    tableSchema(1, List.of(columnFromType(typeBefore))),
                    tableSchema(2, List.of(columnFromType(typeAfter)))
            );
        }

        private static CatalogTableColumnDescriptor columnFromType(Type type) {
            return new CatalogTableColumnDescriptor(
                    "col",
                    type.columnType,
                    true,
                    type.precision,
                    type.scale,
                    type.length,
                    null
            );
        }

        @Override
        public String toString() {
            return String.format("%s -> %s", typeBefore, typeAfter);
        }
    }
}
