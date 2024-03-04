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
import java.util.Set;
import java.util.stream.Stream;
import org.apache.ignite.internal.catalog.CatalogValidationException;
import org.apache.ignite.internal.catalog.commands.CatalogPrimaryKey.CatalogPrimaryKeyBuilder;
import org.apache.ignite.internal.catalog.descriptors.CatalogColumnCollation;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

/** Tests for {@link CatalogPrimaryKey} and its subclasses. */
public class CatalogPrimaryKeyTest extends BaseIgniteAbstractTest {

    @ParameterizedTest
    @MethodSource("primaryKeys")
    void pkCanNotIncludeDuplicateColumns(CatalogPrimaryKeyBuilder<?> builder) {
        CatalogPrimaryKey pk = builder.columns(List.of("C1", "C2", "C1")).build();

        assertThrowsWithCause(
                () -> pk.validate(Set.of("C1", "C2", "C3")),
                CatalogValidationException.class,
                "PK column 'C1' specified more that once"
        );
    }

    @ParameterizedTest
    @MethodSource("primaryKeys")
    void pkCanNotIncludeUnknownColumns(CatalogPrimaryKeyBuilder<?> builder) {
        CatalogPrimaryKey pk = builder.columns(List.of("C1", "foo")).build();

        assertThrowsWithCause(
                () -> pk.validate(Set.of("C1", "C2", "C3")),
                CatalogValidationException.class,
                "PK column 'foo' is not part of table"
        );
    }

    private static Stream<Arguments> primaryKeys() {
        return Stream.of(
                Arguments.of(named("hash pk builder", CatalogHashPrimaryKey.builder())),
                Arguments.of(named("sorted pk builder", CatalogSortedPrimaryKey.builder()))
        );
    }

    @ParameterizedTest
    @MethodSource("emptyHashPrimaryKeys")
    void hashPkWithNoColumns(CatalogHashPrimaryKey.Builder builder) {
        // PK is not validated in build method,
        // but validated in CatalogPrimaryKey::validate.
        CatalogHashPrimaryKey pk = builder.build();

        assertEquals(List.of(), pk.columns());
    }

    private static Stream<Arguments> emptyHashPrimaryKeys() {
        return Stream.of(
                Arguments.of(named("hash pk builder", CatalogHashPrimaryKey.builder())),
                Arguments.of(named("hash pk builder empty columns", CatalogHashPrimaryKey.builder().columns(List.of())))
        );
    }

    @Test
    void hashPk() {
        CatalogHashPrimaryKey pk = CatalogHashPrimaryKey.builder()
                .columns(List.of("C1", "C2"))
                .build();

        assertEquals(List.of("C1", "C2"), pk.columns());
    }

    @ParameterizedTest
    @MethodSource("emptySortedPrimaryKeys")
    void sortedPkWithNoColumns(CatalogSortedPrimaryKey.Builder builder) {
        // PK is not validated in build method,
        // but validated in CatalogPrimaryKey::validate.
        CatalogSortedPrimaryKey pk = builder.build();

        assertEquals(List.of(), pk.columns());
        assertEquals(List.of(), pk.collations());
    }

    private static Stream<Arguments> emptySortedPrimaryKeys() {
        return Stream.of(
                Arguments.of(named("sorted pk builder", CatalogSortedPrimaryKey.builder())),
                Arguments.of(named("sorted pk builder empty columns", CatalogSortedPrimaryKey.builder().columns(List.of()))),
                Arguments.of(named("sorted pk builder empty collations", CatalogSortedPrimaryKey.builder().collations(List.of())))
        );
    }

    @ParameterizedTest
    @EnumSource(CatalogColumnCollation.class)
    void sortedPkWithCollations(CatalogColumnCollation collation) {

        CatalogColumnCollation otherCollation = Arrays.stream(CatalogColumnCollation.values())
                .filter(c -> c != collation).findAny().get();

        CatalogSortedPrimaryKey pk = CatalogSortedPrimaryKey.builder()
                .columns(List.of("C1", "C2"))
                .collations(List.of(collation, otherCollation))
                .build();

        pk.validate(Set.of("C1", "C2", "C3"));

        assertEquals(List.of("C1", "C2"), pk.columns());
        assertEquals(List.of(collation, otherCollation), pk.collations(), "collations");
    }

    @Test
    void sortedPkShouldHaveTheSameNumberOfColumnsAndCollations() {
        CatalogSortedPrimaryKey pk = CatalogSortedPrimaryKey.builder()
                .columns(List.of("C1", "C2"))
                .collations(List.of(CatalogColumnCollation.ASC_NULLS_LAST))
                .build();

        assertThrowsWithCause(
                () -> pk.validate(Set.of("C1", "C2", "C3")),
                CatalogValidationException.class,
                "Number of collations does not match"
        );
    }
}
