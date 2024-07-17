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

import java.util.List;
import org.apache.ignite.internal.catalog.CatalogValidationException;
import org.apache.ignite.internal.catalog.descriptors.CatalogColumnCollation;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests to verify validation of {@link CreateSortedIndexCommand}.
 */
@SuppressWarnings("ThrowableNotThrown")
public class CreateSortedIndexCommandValidationTest extends CreateAbstractIndexCommandValidationTest {
    @Override
    protected <T extends AbstractCreateIndexCommandBuilder<T>> T prefilledBuilder() {
        return (T) fillBuilder(CreateSortedIndexCommand.builder());
    }

    @ParameterizedTest(name = "[{index}] {argumentsWithNames}")
    @MethodSource("nullAndEmptyLists")
    void collationsShouldBeSpecified(List<CatalogColumnCollation> collations) {
        CreateSortedIndexCommandBuilder builder = fillBuilder(CreateSortedIndexCommand.builder());

        builder.collations(collations);

        assertThrowsWithCause(
                builder::build,
                CatalogValidationException.class,
                "Collations not specified"
        );
    }

    @Test
    void collationsSizeShouldMatchColumnsSize() {
        CreateSortedIndexCommandBuilder builder = fillBuilder(CreateSortedIndexCommand.builder())
                .columns(List.of("C1", "C2"));

        List<CatalogColumnCollation> collations = List.of(
                CatalogColumnCollation.ASC_NULLS_FIRST,
                CatalogColumnCollation.ASC_NULLS_FIRST,
                CatalogColumnCollation.ASC_NULLS_FIRST
        );

        builder.collations(collations);

        assertThrowsWithCause(
                builder::build,
                CatalogValidationException.class,
                "Columns collations doesn't match number of columns"
        );

        builder.collations(collations.subList(0, 1));

        assertThrowsWithCause(
                builder::build,
                CatalogValidationException.class,
                "Columns collations doesn't match number of columns"
        );
    }

    private static CreateSortedIndexCommandBuilder fillBuilder(CreateSortedIndexCommandBuilder builder) {
        return builder.schemaName(SCHEMA_NAME)
                .indexName(INDEX_NAME)
                .tableName(TABLE_NAME)
                .columns(List.of("VAL"))
                .collations(List.of(CatalogColumnCollation.ASC_NULLS_FIRST));
    }
}
