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

package org.apache.ignite.internal.catalog.sql;

import static java.util.Collections.singletonList;
import static org.apache.ignite.catalog.ColumnType.INTEGER;
import static org.apache.ignite.catalog.IndexType.DEFAULT;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrows;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.apache.ignite.catalog.ColumnSorted;
import org.apache.ignite.catalog.IndexType;
import org.apache.ignite.catalog.SortOrder;
import org.apache.ignite.catalog.definitions.ColumnDefinition;
import org.apache.ignite.catalog.definitions.TableDefinition;
import org.apache.ignite.catalog.definitions.ZoneDefinition;
import org.junit.jupiter.api.Test;

@SuppressWarnings("ThrowableNotThrown")
class InvalidDefinitionTest {
    @Test
    void zoneName() {
        assertNullOrBlank(ZoneDefinition::builder, "zone", "Zone name");
    }

    @Test
    void zone() {
        assertZoneBuilderNull(ZoneDefinition.Builder::partitions, 1, "Number of partitions");
        assertZoneBuilderNull(ZoneDefinition.Builder::replicas, 1, "Number of replicas");
        assertZoneBuilderNull(ZoneDefinition.Builder::quorumSize, 1, "Quorum size");

        assertZoneBuilderNullOrBlank(ZoneDefinition.Builder::distributionAlgorithm, "a", "Partition distribution algorithm");

        assertZoneBuilderNull(ZoneDefinition.Builder::dataNodesAutoAdjustScaleUp, 1,
                "Timeout between node added topology event itself and data nodes switch");
        assertZoneBuilderNull(ZoneDefinition.Builder::dataNodesAutoAdjustScaleDown, 1,
                "Timeout between node left topology event itself and data nodes switch");

        assertZoneBuilderNullOrBlank(ZoneDefinition.Builder::filter, "f", "Filter");
    }

    @Test
    void tableName() {
        assertNullOrBlank(TableDefinition::builder, "table", "Table name");
    }

    @Test
    void columns() {
        ColumnDefinition validColumn = ColumnDefinition.column("id", INTEGER);

        assertTableBuilderNull(TableDefinition.Builder::columns, new ColumnDefinition[]{validColumn}, "Columns array");
        assertTableBuilderNull(TableDefinition.Builder::columns, singletonList(validColumn), "Columns list");

        assertThrows(NullPointerException.class,
                () -> tableBuilder().columns(new ColumnDefinition[]{null}),
                "Column must not be null.");

        assertThrows(NullPointerException.class,
                () -> tableBuilder().columns(singletonList(null)),
                "Column must not be null.");
    }

    @Test
    void colocateBy() {
        assertTableBuilderNull(TableDefinition.Builder::colocateBy, new String[]{"id"}, "Colocation columns array");
        assertTableBuilderNull(TableDefinition.Builder::colocateBy, singletonList("id"), "Colocation columns list");

        assertThrows(NullPointerException.class,
                () -> tableBuilder().colocateBy(new String[]{null}),
                "Colocation column must not be null.");

        assertThrows(NullPointerException.class,
                () -> tableBuilder().colocateBy(singletonList(null)),
                "Colocation column must not be null.");
    }

    @Test
    void primaryKey() {
        assertTableBuilderNull(TableDefinition.Builder::primaryKey, new String[]{"id"}, "Primary key columns array");

        ColumnSorted validColumn = ColumnSorted.column("id");

        assertTableBuilderNull((builder, columns) -> builder.primaryKey(DEFAULT, columns), new ColumnSorted[]{validColumn},
                "Primary key columns array");
        assertTableBuilderNull((builder, columns) -> builder.primaryKey(DEFAULT, columns), singletonList(validColumn),
                "Primary key columns list");

        assertTableBuilderNull((builder, type) -> builder.primaryKey(type, new ColumnSorted[]{validColumn}), DEFAULT,
                "Primary key index type");
        assertTableBuilderNull((builder, type) -> builder.primaryKey(type, singletonList(validColumn)), DEFAULT,
                "Primary key index type");

        assertThrows(NullPointerException.class,
                () -> tableBuilder().primaryKey((String) null),
                "Primary key column must not be null.");

        assertThrows(NullPointerException.class,
                () -> tableBuilder().primaryKey(DEFAULT, new ColumnSorted[]{null}),
                "Primary key column must not be null.");

        assertThrows(NullPointerException.class,
                () -> tableBuilder().primaryKey(DEFAULT, singletonList(null)),
                "Primary key column must not be null.");
    }

    @Test
    void index() {
        assertTableBuilderNull(TableDefinition.Builder::index, new String[]{"id"}, "Index columns array");

        ColumnSorted validColumn = ColumnSorted.column("id");

        assertTableBuilderNull((builder, columns) -> builder.index(null, DEFAULT, columns), new ColumnSorted[]{validColumn},
                "Index columns array");
        assertTableBuilderNull((builder, columns) -> builder.index(null, DEFAULT, columns), singletonList(validColumn),
                "Index columns list");

        assertTableBuilderNull((builder, type) -> builder.index(null, type, new ColumnSorted[]{validColumn}), DEFAULT,
                "Index type");
        assertTableBuilderNull((builder, type) -> builder.index(null, type, singletonList(validColumn)), DEFAULT,
                "Index type");

        assertThrows(NullPointerException.class,
                () -> tableBuilder().index(new String[]{null}),
                "Index column must not be null.");

        assertThrows(NullPointerException.class,
                () -> tableBuilder().index(null, DEFAULT, new ColumnSorted[]{null}),
                "Index column must not be null.");

        assertThrows(NullPointerException.class,
                () -> tableBuilder().index(null, DEFAULT, singletonList(null)),
                "Index column must not be null.");

        SortOrder[] invalidSortOrders = {
                SortOrder.DESC,
                SortOrder.DESC_NULLS_FIRST,
                SortOrder.DESC_NULLS_LAST,
                SortOrder.ASC,
                SortOrder.ASC_NULLS_FIRST,
                SortOrder.ASC_NULLS_LAST,
                SortOrder.NULLS_FIRST,
                SortOrder.NULLS_LAST
        };

        for (SortOrder order : invalidSortOrders) {
            assertThrows(
                    IllegalArgumentException.class,
                    () -> tableBuilder()
                            .index(null, IndexType.HASH, ColumnSorted.column(validColumn.columnName(), order))
                            .build(),
                    "Index columns must not define a sort order in hash indexes."
            );
        }
    }

    @Test
    void indexColumnsEmptyListMustFail() {
        assertThrows(IllegalArgumentException.class,
                () -> tableBuilder().index("idx", DEFAULT),
                "Index columns list must not be empty.");
    }

    @Test
    void indexColumnWhitespaceMustFail() {
        assertThrows(IllegalArgumentException.class,
                () -> tableBuilder().index(""),
                "Index column must not be blank.");

        assertThrows(IllegalArgumentException.class,
                () -> tableBuilder().index(" "),
                "Index column must not be blank.");

        assertThrows(IllegalArgumentException.class,
                () -> tableBuilder().index("   "),
                "Index column must not be blank.");

        assertThrows(IllegalArgumentException.class,
                () -> tableBuilder().index("col1", "   "),
                "Index column must not be blank.");
    }

    @Test
    void columnDefinition() {
        assertNullOrBlank(name -> ColumnDefinition.column(name, INTEGER), "column", "Column name");
        assertNullOrBlank(name -> ColumnDefinition.column(name, "definition"), "column", "Column name");

        assertNull(type -> ColumnDefinition.column("column", type), INTEGER, "Column type");
        assertNullOrBlank(def -> ColumnDefinition.column("column", def), "definition", "Column definition");
    }

    private static ZoneDefinition.Builder zoneBuilder() {
        return ZoneDefinition.builder("zone");
    }

    private static TableDefinition.Builder tableBuilder() {
        return TableDefinition.builder("table");
    }

    /**
     * Checks that the {@link ZoneDefinition.Builder} method throws {@link NullPointerException} when passed a null value and doesn't throw
     * when passed a valid value.
     *
     * @param builderConsumer Function which takes a builder as a first argument and a tested value as a second argument.
     * @param validValue Valid value.
     * @param messageFragment Fragment of the error text in the exception.
     * @param <T> Type of the second argument.
     */
    private static <T> void assertZoneBuilderNull(
            BiConsumer<ZoneDefinition.Builder, T> builderConsumer,
            T validValue,
            String messageFragment
    ) {
        assertNull(InvalidDefinitionTest::zoneBuilder, builderConsumer, validValue, messageFragment);
    }

    /**
     * Checks that the {@link ZoneDefinition.Builder} method throws {@link NullPointerException} when passed a null string,
     * {@link IllegalArgumentException} when passed a blank string and doesn't throw when passed a valid value.
     *
     * @param builderConsumer Function which takes a builder as a first argument and a tested value as a second argument.
     * @param validValue Valid value.
     * @param messageFragment Fragment of the error text in the exception.
     */
    private static void assertZoneBuilderNullOrBlank(
            BiConsumer<ZoneDefinition.Builder, String> builderConsumer,
            String validValue,
            String messageFragment
    ) {
        assertNull(InvalidDefinitionTest::zoneBuilder, builderConsumer, validValue, messageFragment);
        assertBlank(InvalidDefinitionTest::zoneBuilder, builderConsumer, messageFragment);
    }

    /**
     * Checks that the {@link TableDefinition.Builder} method throws {@link NullPointerException} when passed a null value and doesn't throw
     * when passed a valid value.
     *
     * @param builderConsumer Function which takes a builder as a first argument and a tested value as a second argument.
     * @param validValue Valid value.
     * @param messageFragment Fragment of the error text in the exception.
     * @param <T> Type of the second argument.
     */
    private static <T> void assertTableBuilderNull(
            BiConsumer<TableDefinition.Builder, T> builderConsumer,
            T validValue,
            String messageFragment
    ) {
        assertNull(InvalidDefinitionTest::tableBuilder, builderConsumer, validValue, messageFragment);
    }

    /**
     * Checks that the function throws {@link NullPointerException} when passed a null value as a second argument and doesn't throw when
     * passed a valid value.
     *
     * @param firstArgumentSupplier Supplier of first argument.
     * @param testedFunction Function to test.
     * @param validValue Valid value.
     * @param messageFragment Fragment of the error text in the exception.
     * @param <T> First argument type.
     * @param <U> Second argument type.
     */
    private static <T, U> void assertNull(
            Supplier<T> firstArgumentSupplier,
            BiConsumer<T, U> testedFunction,
            U validValue,
            String messageFragment
    ) {
        assertThrows(
                NullPointerException.class,
                () -> testedFunction.accept(firstArgumentSupplier.get(), null),
                messageFragment + " must not be null."
        );
        assertDoesNotThrow(() -> testedFunction.accept(firstArgumentSupplier.get(), validValue));
    }

    /**
     * Checks that the function throws {@link NullPointerException} when passed a null value and doesn't throw when passed a valid value.
     *
     * @param consumer Function to test.
     * @param validValue Valid value.
     * @param messageFragment Fragment of the error text in the exception.
     */
    private static <T> void assertNull(Consumer<T> consumer, T validValue, String messageFragment) {
        assertNull(() -> null, (unused, value) -> consumer.accept(value), validValue, messageFragment);
    }

    /**
     * Checks that the function throws {@link NullPointerException} when passed a null string, {@link IllegalArgumentException} when passed
     * a blank string and doesn't throw when passed a valid value.
     *
     * @param consumer Function to test.
     * @param validValue Valid value.
     * @param messageFragment Fragment of the error text in the exception.
     */
    private static void assertNullOrBlank(Consumer<String> consumer, String validValue, String messageFragment) {
        assertNull(() -> null, (unused, value) -> consumer.accept(value), validValue, messageFragment);
        assertBlank(() -> null, (unused, value) -> consumer.accept(value), messageFragment);
    }

    /**
     * Checks that the function throws {@link IllegalArgumentException} when passed a blank string as a second argument.
     *
     * @param firstArgumentSupplier Supplier of first argument.
     * @param testedFunction Function to test.
     * @param messageFragment Fragment of the error text in the exception.
     * @param <T> First argument type.
     */
    private static <T> void assertBlank(
            Supplier<T> firstArgumentSupplier,
            BiConsumer<T, String> testedFunction,
            String messageFragment
    ) {
        assertThrows(
                IllegalArgumentException.class,
                () -> testedFunction.accept(firstArgumentSupplier.get(), ""),
                messageFragment + " must not be blank."
        );
    }
}
