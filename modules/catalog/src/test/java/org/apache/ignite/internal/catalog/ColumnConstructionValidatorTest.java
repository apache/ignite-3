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

package org.apache.ignite.internal.catalog;

import static org.apache.ignite.internal.catalog.CatalogTestUtils.DEFAULT_NULLABLE;
import static org.apache.ignite.internal.catalog.CatalogTestUtils.applyNecessaryLength;
import static org.apache.ignite.internal.catalog.CatalogTestUtils.columnParamsBuilder;
import static org.apache.ignite.internal.catalog.CatalogTestUtils.initializeColumnWithDefaults;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrowsWithCause;

import org.apache.ignite.internal.catalog.commands.ColumnParams;
import org.apache.ignite.internal.catalog.commands.ColumnParams.Builder;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.sql.ColumnType;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.EnumSource.Mode;

/** Test {@link ColumnParams} assembly and validation. */
public class ColumnConstructionValidatorTest extends BaseIgniteAbstractTest {
    @ParameterizedTest
    @EnumSource(value = ColumnType.class, names = "NULL", mode = Mode.EXCLUDE)
    public void testColumnSetPrecisionScale(ColumnType type) {
        Builder colBuilder = columnParamsBuilder("COL", type, DEFAULT_NULLABLE, 20);

        Builder colBuilderErrPrec = columnParamsBuilder("COL", type, DEFAULT_NULLABLE, -1, 2);
        Builder colBuilderErrScale = columnParamsBuilder("COL", type, DEFAULT_NULLABLE, 20, -1);

        applyNecessaryLength(type, colBuilder);

        if (!type.precisionAllowed() && !type.scaleAllowed()) {
            assertThrowsWithCause(colBuilder::build, CatalogValidationException.class, "Precision is not applicable for column");
        }

        if (type.precisionAllowed() && !type.scaleAllowed()) {
            Builder colBuilder0 = columnParamsBuilder("COL", type, DEFAULT_NULLABLE, 20, 2);
            assertThrowsWithCause(colBuilder0::build, CatalogValidationException.class, "Scale is not applicable for column");
        }

        if (type.precisionAllowed()) {
            assertThrowsWithCause(colBuilderErrPrec::build, CatalogValidationException.class);
        }

        if (type.scaleAllowed()) {
            assertThrowsWithCause(colBuilderErrScale::build, CatalogValidationException.class);
        }
    }

    @ParameterizedTest
    @EnumSource(value = ColumnType.class, names = "NULL", mode = Mode.EXCLUDE)
    public void testColumnSetLength(ColumnType type) {
        Builder colBuilder = columnParamsBuilder("COL", type, DEFAULT_NULLABLE);
        Builder colBuilder1 = columnParamsBuilder("COL1", type, DEFAULT_NULLABLE);

        initializeColumnWithDefaults(type, colBuilder);
        initializeColumnWithDefaults(type, colBuilder1);

        colBuilder.length(100);

        if (!type.specifiedLength()) {
            assertThrowsWithCause(colBuilder::build, CatalogValidationException.class, "Length specification is not applicable");
            return;
        }

        colBuilder.build();

        colBuilder1.length(-1);
        assertThrowsWithCause(colBuilder1::build, CatalogValidationException.class);
    }
}
