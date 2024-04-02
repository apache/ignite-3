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

package org.apache.ignite.internal.catalog.commands;

import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrowsWithCause;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Named.named;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.internal.catalog.CatalogValidationException;
import org.apache.ignite.internal.catalog.commands.TablePrimaryKey.TablePrimaryKeyBuilder;
import org.apache.ignite.internal.catalog.descriptors.CatalogColumnCollation;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.sql.ColumnType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

/** Tests for {@link TablePrimaryKey} and its subclasses. */
public class TablePrimaryKeyTest extends BaseIgniteAbstractTest {

    @ParameterizedTest
    @MethodSource("primaryKeys")
    void pkCanNotIncludeDuplicateColumns(TablePrimaryKeyBuilder<?> builder) {
        TablePrimaryKey pk = builder.columns(List.of("C1", "C2", "C1")).build();

        assertThrowsWithCause(
                () -> pk.validate(createSimpleColumnParams("C1", "C2", "C3")),
                CatalogValidationException.class,
                "PK column 'C1' specified more that once."
        );
    }

    @ParameterizedTest
    @MethodSource("primaryKeys")
    void pkCanNotIncludeUnknownColumns(TablePrimaryKeyBuilder<?> builder) {
        TablePrimaryKey pk = builder.columns(List.of("C1", "foo")).build();

        assertThrowsWithCause(
                () -> pk.validate(createSimpleColumnParams("C1", "C2", "C3")),
                CatalogValidationException.class,
                "Primary key constraint contains undefined columns: [cols=[foo]]."
        );
    }

    private static Stream<Arguments> primaryKeys() {
        return Stream.of(
                Arguments.of(named("hash pk builder", TableHashPrimaryKey.builder())),
                Arguments.of(named("sorted pk builder", TableSortedPrimaryKey.builder()))
        );
    }

    @ParameterizedTest
    @MethodSource("emptyHashPrimaryKeys")
    void hashPkWithNoColumns(TableHashPrimaryKey.Builder builder) {
        // PK is not validated in build method,
        // but validated in CatalogPrimaryKey::validate.
        TableHashPrimaryKey pk = builder.build();

        assertEquals(List.of(), pk.columns());
    }

    private static Stream<Arguments> emptyHashPrimaryKeys() {
        return Stream.of(
                Arguments.of(named("hash pk builder", TableHashPrimaryKey.builder())),
                Arguments.of(named("hash pk builder empty columns", TableHashPrimaryKey.builder().columns(List.of())))
        );
    }

    @Test
    void hashPk() {
        TableHashPrimaryKey pk = TableHashPrimaryKey.builder()
                .columns(List.of("C1", "C2"))
                .build();

        assertEquals(List.of("C1", "C2"), pk.columns());
    }

    @ParameterizedTest
    @MethodSource("emptySortedPrimaryKeys")
    void sortedPkWithNoColumns(TableSortedPrimaryKey.Builder builder) {
        // PK is not validated in build method,
        // but validated in CatalogPrimaryKey::validate.
        TableSortedPrimaryKey pk = builder.build();

        assertEquals(List.of(), pk.columns());
        assertEquals(List.of(), pk.collations());
    }

    private static Stream<Arguments> emptySortedPrimaryKeys() {
        return Stream.of(
                Arguments.of(named("sorted pk builder", TableSortedPrimaryKey.builder())),
                Arguments.of(named("sorted pk builder empty columns", TableSortedPrimaryKey.builder().columns(List.of()))),
                Arguments.of(named("sorted pk builder empty collations", TableSortedPrimaryKey.builder().collations(List.of())))
        );
    }

    @ParameterizedTest
    @EnumSource(CatalogColumnCollation.class)
    void sortedPkWithCollations(CatalogColumnCollation collation) {

        CatalogColumnCollation otherCollation = Arrays.stream(CatalogColumnCollation.values())
                .filter(c -> c != collation).findAny().get();

        TableSortedPrimaryKey pk = TableSortedPrimaryKey.builder()
                .columns(List.of("C1", "C2"))
                .collations(List.of(collation, otherCollation))
                .build();

        pk.validate(createSimpleColumnParams("C1", "C2", "C3"));

        assertEquals(List.of("C1", "C2"), pk.columns());
        assertEquals(List.of(collation, otherCollation), pk.collations(), "collations");
    }

    @Test
    void sortedPkShouldHaveTheSameNumberOfColumnsAndCollations() {
        TableSortedPrimaryKey pk = TableSortedPrimaryKey.builder()
                .columns(List.of("C1", "C2"))
                .collations(List.of(CatalogColumnCollation.ASC_NULLS_LAST))
                .build();

        assertThrowsWithCause(
                () -> pk.validate(createSimpleColumnParams("C1", "C2", "C3")),
                CatalogValidationException.class,
                "Number of collations does not match."
        );
    }

    private List<ColumnParams> createSimpleColumnParams(String... columns) {
        return Arrays.stream(columns).map(col -> ColumnParams.builder().name(col).type(ColumnType.INT8).build())
                .collect(Collectors.toList());
    }
}
