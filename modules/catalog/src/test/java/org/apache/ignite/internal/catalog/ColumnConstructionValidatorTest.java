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
import static org.apache.ignite.internal.catalog.CatalogTestUtils.columnParamsBuilder;
import static org.apache.ignite.internal.catalog.CatalogTestUtils.initializeColumnWithDefaults;
import static org.apache.ignite.internal.catalog.commands.ColumnParams.ERR_COL_PARAM_DEFINITION;
import static org.apache.ignite.internal.catalog.commands.ColumnParams.ERR_COL_PARAM_NOT_APPLICABLE;
import static org.apache.ignite.internal.catalog.commands.ColumnParams.ERR_COL_PARAM_VALIDATION;
import static org.apache.ignite.internal.catalog.commands.ColumnParams.ERR_COL_POSITIVE_PARAM_VALIDATION;
import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrowsWithCause;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.apache.ignite.internal.catalog.commands.ColumnParams;
import org.apache.ignite.internal.catalog.commands.ColumnParams.Builder;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.sql.ColumnType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.EnumSource.Mode;

/** Test {@link ColumnParams} assembly and validation. */
public class ColumnConstructionValidatorTest extends BaseIgniteAbstractTest {
    @ParameterizedTest
    @EnumSource(value = ColumnType.class, names = "NULL", mode = Mode.EXCLUDE)
    public void testColumnSetPrecision(ColumnType type) {
        Builder noPrecision = columnParamsBuilder("COL", type, DEFAULT_NULLABLE);
        Builder invalidPrecision = columnParamsBuilder("COL", type, DEFAULT_NULLABLE);
        Builder correctPrecision = columnParamsBuilder("COL", type, DEFAULT_NULLABLE);

        initializeColumnWithDefaults(type, noPrecision);
        initializeColumnWithDefaults(type, invalidPrecision);
        initializeColumnWithDefaults(type, correctPrecision);

        noPrecision.precision(null);
        invalidPrecision.precision(-1);

        if (type.precisionAllowed()) {
            assertThrowsWithCause(noPrecision::build, CatalogValidationException.class,
                    format(ERR_COL_PARAM_DEFINITION, "Precision", "COL", type.name()));

            assertThrowsWithCause(invalidPrecision::build, CatalogValidationException.class,
                    format(ERR_COL_PARAM_VALIDATION, "Precision", "COL", type.name()));

            ColumnParams col = correctPrecision.build();
            assertNotNull(col.precision());
        } else {
            correctPrecision.precision(1);

            assertThrowsWithCause(correctPrecision::build, CatalogValidationException.class,
                    format(ERR_COL_PARAM_NOT_APPLICABLE, "Precision", "COL", type.name()));
        }
    }

    @ParameterizedTest
    @EnumSource(value = ColumnType.class, names = "NULL", mode = Mode.EXCLUDE)
    public void testColumnSetScale(ColumnType type) {
        Builder noScale = columnParamsBuilder("COL", type, DEFAULT_NULLABLE);
        Builder invalidScale = columnParamsBuilder("COL", type, DEFAULT_NULLABLE);
        Builder correctScale = columnParamsBuilder("COL", type, DEFAULT_NULLABLE);

        initializeColumnWithDefaults(type, noScale);
        initializeColumnWithDefaults(type, invalidScale);
        initializeColumnWithDefaults(type, correctScale);

        noScale.scale(null);
        invalidScale.scale(-1);

        if (type.scaleAllowed()) {
            assertThrowsWithCause(noScale::build, CatalogValidationException.class,
                    format(ERR_COL_PARAM_DEFINITION, "Scale", "COL", type.name()));

            assertThrowsWithCause(invalidScale::build, CatalogValidationException.class,
                    format(ERR_COL_PARAM_VALIDATION, "Scale", "COL", type.name()));

            ColumnParams col = correctScale.build();
            assertNotNull(col.scale());
        } else {
            correctScale.scale(1);

            assertThrowsWithCause(correctScale::build, CatalogValidationException.class,
                    format(ERR_COL_PARAM_NOT_APPLICABLE, "Scale", "COL", type.name()));
        }
    }

    @ParameterizedTest
    @EnumSource(value = ColumnType.class, names = "NULL", mode = Mode.EXCLUDE)
    public void testColumnSetLength(ColumnType type) {
        Builder noLength = columnParamsBuilder("COL", type, DEFAULT_NULLABLE);
        Builder negativeLength = columnParamsBuilder("COL", type, DEFAULT_NULLABLE);
        Builder correctLength = columnParamsBuilder("COL", type, DEFAULT_NULLABLE);
        Builder zeroLength = columnParamsBuilder("COL", type, DEFAULT_NULLABLE);

        initializeColumnWithDefaults(type, noLength);
        initializeColumnWithDefaults(type, negativeLength);
        initializeColumnWithDefaults(type, correctLength);
        initializeColumnWithDefaults(type, zeroLength);

        noLength.length(null);
        negativeLength.length(-1);
        zeroLength.length(0);
        
        if (type.lengthAllowed()) {
            assertThrowsWithCause(noLength::build, CatalogValidationException.class,
                    format(ERR_COL_PARAM_DEFINITION, "Length", "COL", type.name()));
            
            assertThrowsWithCause(negativeLength::build, CatalogValidationException.class,
                    format(ERR_COL_POSITIVE_PARAM_VALIDATION, "Length", "COL", type.name()));

            assertThrowsWithCause(zeroLength::build, CatalogValidationException.class,
                    format(ERR_COL_POSITIVE_PARAM_VALIDATION, "Length", "COL", type.name()));

            ColumnParams col = correctLength.build();
            assertNotNull(col.length());
        } else {
            correctLength.length(1);

            assertThrowsWithCause(correctLength::build, CatalogValidationException.class,
                    format(ERR_COL_PARAM_NOT_APPLICABLE, "Length", "COL", type.name()));
        }
    }

    @ParameterizedTest
    @EnumSource(value = ColumnType.class, names = "NULL", mode = Mode.EXCLUDE)
    public void testColumnEmptyName(ColumnType type) {
        Builder colBuilder = columnParamsBuilder(null, type, DEFAULT_NULLABLE);
        initializeColumnWithDefaults(type, colBuilder);

        assertThrowsWithCause(colBuilder::build, CatalogValidationException.class, "Column name");
    }

    @Test
    public void testColumnEmptyType() {
        Builder colBuilder = columnParamsBuilder("COL", null, DEFAULT_NULLABLE);

        assertThrowsWithCause(colBuilder::build, CatalogValidationException.class, "Type is not specified for column 'COL'");
    }
}
