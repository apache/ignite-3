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

package org.apache.ignite.internal.partition.replicator.schemacompat;

import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.DEFAULT_LENGTH;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.DEFAULT_PRECISION;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.DEFAULT_SCALE;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.tx.TransactionIds.transactionId;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
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
import static org.apache.ignite.sql.ColumnType.PERIOD;
import static org.apache.ignite.sql.ColumnType.STRING;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntMaps;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.catalog.commands.DefaultValue;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableColumnDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.IgniteBiTuple;
import org.apache.ignite.internal.partition.replicator.schema.FullTableSchema;
import org.apache.ignite.internal.partition.replicator.schema.ValidationSchemasSource;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.schema.AlwaysSyncedSchemaSyncService;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.sql.ColumnType;
import org.jetbrains.annotations.Nullable;
import org.jspecify.annotations.NonNull;
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

    private final UUID txId = transactionId(beginTimestamp, 0);

    private static final int TABLE_ID = 1;
    private static final String TABLE_NAME = "test";
    private static final String ANOTHER_NAME = "another";

    private final TablePartitionId tablePartitionId = new TablePartitionId(TABLE_ID, 0);

    @BeforeEach
    void createValidatorAndInitMocks() {
        Catalog catalog = mock(Catalog.class);
        lenient().when(catalog.table(TABLE_ID)).thenReturn(mock(CatalogTableDescriptor.class));
        lenient().when(catalogService.activeCatalog(commitTimestamp.longValue())).thenReturn(catalog);

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

        CompletableFuture<CompatValidationResult> resultFuture = validator.validateCommit(
                txId,
                Set.of(tablePartitionId.tableId()),
                commitTimestamp
        );

        assertThat(resultFuture, willCompleteSuccessfully());

        CompatValidationResult result = resultFuture.getNow(null);
        assertThat(result, is(notNullValue()));

        assertThat("Change is incompatible: " + result.optionalDetails(), result.isSuccessful(), is(true));
    }

    @ParameterizedTest
    @EnumSource(ForwardIncompatibleChange.class)
    void forwardIncompatibleChangesDisallowCommitting(ForwardIncompatibleChange change) {
        assertForwardIncompatibleChangeDisallowsCommitting(change);
    }

    private void assertForwardIncompatibleChangeDisallowsCommitting(SchemaChangeSource changeSource) {
        when(schemasSource.tableSchemaVersionsBetween(TABLE_ID, beginTimestamp, commitTimestamp))
                .thenReturn(changeSource.schemaVersions());

        CompletableFuture<CompatValidationResult> resultFuture = validator.validateCommit(
                txId,
                Set.of(tablePartitionId.tableId()),
                commitTimestamp
        );

        assertThat(resultFuture, willCompleteSuccessfully());

        CompatValidationResult result = resultFuture.getNow(null);
        assertThat(result, is(notNullValue()));

        assertThat("Change should be incompatible", result.isSuccessful(), is(false));
        assertThat(result.isTableDropped(), is(false));

        assertThat(result.failedTableName(), is(TABLE_NAME));
        assertThat(result.fromSchemaVersion(), is(changeSource.fromSchemaVersion()));
        assertThat(result.toSchemaVersion(), is(2));
        assertThat(result.details(), is(changeSource.expectedDetails()));
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

        changes.add(new ColumnTypeChange(INT8, decimal(100, 0)));
        changes.add(new ColumnTypeChange(INT16, decimal(100, 0)));
        changes.add(new ColumnTypeChange(INT32, decimal(100, 0)));
        changes.add(new ColumnTypeChange(INT64, decimal(100, 0)));

        // Decreasing precision.
        changes.add(new ColumnTypeChange(decimal(10, 5), decimal(9, 5)));

        // Decreasing length.
        changes.add(new ColumnTypeChange(string(10), string(9)));
        changes.add(new ColumnTypeChange(byteArray(10), byteArray(9)));

        // Conversions to STRING.
        changes.add(new ColumnTypeChange(INT8, string(100)));
        changes.add(new ColumnTypeChange(INT16, string(100)));
        changes.add(new ColumnTypeChange(INT32, string(100)));
        changes.add(new ColumnTypeChange(INT64, string(100)));
        changes.add(new ColumnTypeChange(decimal(10, 0), string(100)));
        changes.add(new ColumnTypeChange(ColumnType.UUID, string(100)));

        // Conversions from STRING.
        changes.add(new ColumnTypeChange(string(1), INT8));
        changes.add(new ColumnTypeChange(string(1), INT16));
        changes.add(new ColumnTypeChange(string(1), INT32));
        changes.add(new ColumnTypeChange(string(1), INT64));
        changes.add(new ColumnTypeChange(string(1), decimal(10, 0)));
        changes.add(new ColumnTypeChange(string(36), ColumnType.UUID));

        for (ColumnType columnType : ColumnType.values()) {
            addInconvertible(columnType, BOOLEAN, changes);
            addInconvertible(columnType, ColumnType.UUID, changes);
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
        assertForwardCompatibleChangeAllowsCommitting((CompatibleSchemaChangeSource) () -> List.of(
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
        assertForwardIncompatibleChangeDisallowsCommitting(new SchemaChangeSource() {
            @Override
            public List<FullTableSchema> schemaVersions() {
                return List.of(
                        tableSchema(1, List.of(column(INT32, true))),
                        // Type is widened (compatible), but NOT NULL added (incompatible).
                        tableSchema(2, List.of(column(INT64, false)))
                );
            }

            @Override
            public String expectedDetails() {
                return "Not null added";
            }
        });
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
        assertChangeIsNotBackwardCompatible((CompatibleSchemaChangeSource) () -> List.of(
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

    @Test
    void doesNotCacheStaleResultForDifferentInitialCatalogVersions() {
        HybridTimestamp beginTsA = new HybridTimestamp(1, 1);
        HybridTimestamp beginTsB = new HybridTimestamp(2, 1);
        HybridTimestamp sharedCommitTs = new HybridTimestamp(5, 1);

        UUID txIdA = transactionId(beginTsA, 0);
        UUID txIdB = transactionId(beginTsB, 0);

        Catalog catalogAtCommit = mock(Catalog.class);
        when(catalogAtCommit.table(TABLE_ID)).thenReturn(mock(CatalogTableDescriptor.class));
        when(catalogService.activeCatalog(sharedCommitTs.longValue())).thenReturn(catalogAtCommit);

        List<CatalogTableColumnDescriptor> cols1 = List.of(intColumn("col1"));
        List<CatalogTableColumnDescriptor> cols2 = List.of(intColumn("col1"), nullableIntColumn("col2"));

        // catalogVersion=1: one column, no index.
        FullTableSchema v1 = tableSchema(1, 1, TABLE_NAME, cols1);
        // catalogVersion=2: nullable col2 added; index 10 registered here (not yet building).
        FullTableSchema v2 = tableSchema(2, 2, TABLE_NAME, cols2);
        // catalogVersion=3: same schema, index 10 started building; it was registered at catalog version 2.
        FullTableSchema v3 = tableSchema(3, 2, TABLE_NAME, cols2, Int2IntMaps.singleton(10, 2));

        // Tx A starts at catalog v2 — only sees [v2, v3].
        when(schemasSource.tableSchemaVersionsBetween(TABLE_ID, beginTsA, sharedCommitTs))
                .thenReturn(List.of(v2, v3));
        // Tx B starts at catalog v1 — sees the full range [v1, v2, v3].
        when(schemasSource.tableSchemaVersionsBetween(TABLE_ID, beginTsB, sharedCommitTs))
                .thenReturn(List.of(v1, v2, v3));

        CompletableFuture<CompatValidationResult> resultA = validator.validateCommit(txIdA, Set.of(TABLE_ID), sharedCommitTs);
        assertThat(resultA, willCompleteSuccessfully());
        assertThat("Tx A should be compatible (it saw the index when it started)",
                resultA.getNow(null).isSuccessful(), is(true));

        CompletableFuture<CompatValidationResult> resultB = validator.validateCommit(txIdB, Set.of(TABLE_ID), sharedCommitTs);
        assertThat(resultB, willCompleteSuccessfully());
        assertThat("Tx B should be incompatible (index was created and started building after Tx B began)",
                resultB.getNow(null).isSuccessful(), is(false));
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

    private static CatalogTableColumnDescriptor int64Column(String columnName) {
        return new CatalogTableColumnDescriptor(
                columnName,
                INT64,
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

    private static CatalogTableColumnDescriptor intColumnWithDefault(String columnName, @Nullable Integer defaultValue) {
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
        return tableSchema(1, schemaVersion, name, columns);
    }

    private static FullTableSchema tableSchema(
            int catalogVersion,
            int schemaVersion,
            String name,
            List<CatalogTableColumnDescriptor> columns
    ) {
        return tableSchema(catalogVersion, schemaVersion, name, columns, Int2IntMaps.EMPTY_MAP);
    }

    private static FullTableSchema tableSchema(
            int catalogVersion,
            int schemaVersion,
            String name,
            List<CatalogTableColumnDescriptor> columns,
            Int2IntMap indexesJustStartedBeingBuilt
    ) {
        return new FullTableSchema(catalogVersion, schemaVersion, TABLE_ID, name, columns, indexesJustStartedBeingBuilt);
    }

    private interface SchemaChangeSource {
        List<FullTableSchema> schemaVersions();

        String expectedDetails();

        default int fromSchemaVersion() {
            return 1;
        }
    }

    @FunctionalInterface
    private interface CompatibleSchemaChangeSource extends SchemaChangeSource {
        @Override
        default @Nullable String expectedDetails() {
            return null;
        }
    }

    private enum ForwardCompatibleChange implements CompatibleSchemaChangeSource {
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
        )),
        PRE_EXISTING_INDEX_STARTED_BUILDING(List.of(
                // In catalog version 1, the index was created.
                tableSchema(1, 1, TABLE_NAME, someColumns()),
                // On this schema, transaction is started (so it sees the index).
                tableSchema(2, 2, TABLE_NAME, someOtherColumns()),
                // On this schema, index was started being built; and exactly on this schema the transaction is tried
                // to be committed.
                tableSchema(3, 2, TABLE_NAME, someOtherColumns(), Int2IntMaps.singleton(10, 1))
        )),
        JUST_REGISTERED_INDEX_STARTED_BUILDING(List.of(
                // On this schema, transaction is started, and the index was created on this schema as well
                // (so the transaction sees the index).
                tableSchema(1, 1, TABLE_NAME, someColumns(), Int2IntMaps.EMPTY_MAP),
                // On this schema, index was started being built; and exactly on this schema the transaction is tried
                // to be committed.
                tableSchema(2, 1, TABLE_NAME, someColumns(), Int2IntMaps.singleton(10, 1))
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

    private static @NonNull List<CatalogTableColumnDescriptor> someOtherColumns() {
        return List.of(intColumn("col1"), nullableIntColumn("col2"));
    }

    private enum ForwardIncompatibleChange implements SchemaChangeSource {
        RENAME_TABLE(
                List.of(
                        tableSchema(1, TABLE_NAME, someColumns()),
                        tableSchema(2, ANOTHER_NAME, someColumns())
                ),
                "Name of the table has been changed"
        ),
        DROP_COLUMN(
                List.of(
                        tableSchema(1, List.of(
                                intColumn("col1"),
                                intColumn("col2")
                        )),
                        tableSchema(2, List.of(
                                intColumn("col1")
                        ))
                ),
                "Columns were dropped"
        ),
        ADD_DEFAULT(
                List.of(
                        tableSchema(1, List.of(
                                intColumn("col1")
                        )),
                        tableSchema(2, List.of(
                                intColumnWithDefault("col1", 42)
                        ))
                ),
                "Column default value changed"
        ),
        CHANGE_DEFAULT(
                List.of(
                        tableSchema(1, List.of(
                                intColumnWithDefault("col1", 1)
                        )),
                        tableSchema(2, List.of(
                                intColumnWithDefault("col1", 2)
                        ))
                ),
                "Column default value changed"
        ),
        DROP_DEFAULT(
                List.of(
                        tableSchema(1, List.of(
                                intColumnWithDefault("col1", 42)
                        )),
                        tableSchema(2, List.of(
                                intColumn("col1")
                        ))
                ),
                "Column default value changed"
        ),
        NON_NULL_WITHOUT_DEFAULT(
                List.of(
                        tableSchema(1, List.of(
                        )),
                        tableSchema(2, List.of(
                                intColumn("NON_NULL_WITHOUT_DEFAULT_COL")
                        ))
                ),
                "Not null column added without default value"
        ),
        NON_NULL_WITH_DEFAULT_NULL(
                List.of(
                        tableSchema(1, List.of(
                        )),
                        tableSchema(2, List.of(
                                intColumnWithDefault("NON_NULL_WITHOUT_DEFAULT_COL", null)
                        ))
                ),
                "Not null column added without default value"
        ),
        TYPE_CHANGED(
                List.of(
                        tableSchema(1, List.of(
                                int64Column("TYPE_CHANGED_COL")
                        )),
                        tableSchema(2, List.of(
                                intColumn("TYPE_CHANGED_COL")
                        ))
                ),
                "Column type changed incompatibly"
        ),
        INDEX_REGISTERED_AND_STARTED_BUILDING(
                List.of(
                        // On this schema, transaction was started, but the index did not exist yet.
                        tableSchema(1, 1, TABLE_NAME, someColumns(), Int2IntMaps.EMPTY_MAP),
                        // On this schema, index was created.
                        tableSchema(2, 2, TABLE_NAME, someOtherColumns(), Int2IntMaps.EMPTY_MAP),
                        // On this schema, index was started being built; and exactly on this schema the transaction is tried
                        // to be committed.
                        tableSchema(3, 2, TABLE_NAME, someOtherColumns(), Int2IntMaps.singleton(10, 2))
                ),
                "Transaction coordinator is stale",
                2
        );

        private final List<FullTableSchema> schemaVersions;
        private final String expectedDetails;
        private final int fromSchemaVersion;

        ForwardIncompatibleChange(List<FullTableSchema> schemaVersions, String expectedDetails) {
            this(schemaVersions, expectedDetails, 1);
        }

        ForwardIncompatibleChange(List<FullTableSchema> schemaVersions, String expectedDetails, int fromSchemaVersion) {
            this.schemaVersions = schemaVersions;
            this.expectedDetails = expectedDetails;
            this.fromSchemaVersion = fromSchemaVersion;
        }

        @Override
        public List<FullTableSchema> schemaVersions() {
            return schemaVersions;
        }

        @Override
        public String expectedDetails() {
            return expectedDetails;
        }

        @Override
        public int fromSchemaVersion() {
            return fromSchemaVersion;
        }
    }

    private static @NonNull List<CatalogTableColumnDescriptor> someColumns() {
        return List.of(intColumn("col1"));
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

        @Override
        public String expectedDetails() {
            return "Column type changed incompatibly";
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
