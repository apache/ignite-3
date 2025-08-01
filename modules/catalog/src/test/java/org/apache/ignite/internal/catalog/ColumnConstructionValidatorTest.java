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
import static org.apache.ignite.internal.catalog.commands.ColumnParams.ERR_COL_INVALID_TYPE_PARAM;
import static org.apache.ignite.internal.catalog.commands.ColumnParams.ERR_COL_PARAM_DEFINITION;
import static org.apache.ignite.internal.catalog.commands.ColumnParams.ERR_COL_PARAM_NOT_APPLICABLE;
import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrowsWithCause;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.apache.ignite.internal.catalog.commands.CatalogUtils;
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
        Builder negativePrecision = columnParamsBuilder("COL", type, DEFAULT_NULLABLE);
        Builder correctPrecision = columnParamsBuilder("COL", type, DEFAULT_NULLABLE);
        Builder tooLargePrecision = columnParamsBuilder("COL", type, DEFAULT_NULLABLE);

        initializeColumnWithDefaults(type, noPrecision);
        initializeColumnWithDefaults(type, negativePrecision);
        initializeColumnWithDefaults(type, correctPrecision);
        initializeColumnWithDefaults(type, tooLargePrecision);

        noPrecision.precision(null);
        negativePrecision.precision(-1);

        if (type.precisionAllowed()) {
            assertThrowsWithCause(noPrecision::build, CatalogValidationException.class,
                    format(ERR_COL_PARAM_DEFINITION, "Precision", "COL", type.name()));

            assertThrowsWithCause(negativePrecision::build, CatalogValidationException.class,
                    TypeParameter.PRECISION.errorMessage(type, "COL", -1));

            int largeValue = CatalogUtils.getMaxPrecision(type) + 1;
            tooLargePrecision.precision(largeValue);

            assertThrowsWithCause(tooLargePrecision::build, CatalogValidationException.class,
                    TypeParameter.PRECISION.errorMessage(type, "COL", largeValue));

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
        Builder negativeScale = columnParamsBuilder("COL", type, DEFAULT_NULLABLE);
        Builder correctScale = columnParamsBuilder("COL", type, DEFAULT_NULLABLE);
        Builder tooLargeScale = columnParamsBuilder("COL", type, DEFAULT_NULLABLE);

        initializeColumnWithDefaults(type, noScale);
        initializeColumnWithDefaults(type, negativeScale);
        initializeColumnWithDefaults(type, correctScale);
        initializeColumnWithDefaults(type, tooLargeScale);

        noScale.scale(null);
        negativeScale.scale(-1);

        if (type.scaleAllowed()) {
            assertThrowsWithCause(noScale::build, CatalogValidationException.class,
                    format(ERR_COL_PARAM_DEFINITION, "Scale", "COL", type.name()));

            assertThrowsWithCause(negativeScale::build, CatalogValidationException.class,
                    TypeParameter.SCALE.errorMessage(type, "COL", -1));

            int largeValue = CatalogUtils.getMaxScale(type) + 1;
            tooLargeScale.scale(largeValue);
            assertThrowsWithCause(tooLargeScale::build, CatalogValidationException.class,
                    TypeParameter.SCALE.errorMessage(type, "COL", largeValue));

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
        Builder zeroLength = columnParamsBuilder("COL", type, DEFAULT_NULLABLE);
        Builder correctLength = columnParamsBuilder("COL", type, DEFAULT_NULLABLE);
        Builder tooLargeLength = columnParamsBuilder("COL", type, DEFAULT_NULLABLE);

        initializeColumnWithDefaults(type, noLength);
        initializeColumnWithDefaults(type, negativeLength);
        initializeColumnWithDefaults(type, correctLength);
        initializeColumnWithDefaults(type, tooLargeLength);

        noLength.length(null);
        zeroLength.length(0);
        negativeLength.length(-1);

        if (type.lengthAllowed()) {
            assertThrowsWithCause(noLength::build, CatalogValidationException.class,
                    format(ERR_COL_PARAM_DEFINITION, "Length", "COL", type.name()));

            assertThrowsWithCause(negativeLength::build, CatalogValidationException.class,
                    TypeParameter.LENGTH.errorMessage(type, "COL", -1));

            assertThrowsWithCause(zeroLength::build, CatalogValidationException.class,
                    TypeParameter.LENGTH.errorMessage(type, "COL", 0));

            int largeValue = CatalogUtils.getMaxLength(type) + 1;
            tooLargeLength.length(largeValue);
            assertThrowsWithCause(tooLargeLength::build, CatalogValidationException.class,
                    TypeParameter.LENGTH.errorMessage(type, "COL", largeValue));

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

    private enum TypeParameter {
        PRECISION,
        SCALE,
        LENGTH;

        String errorMessage(ColumnType type, String column, int value) {
            int min;
            int max;
            String name;
            switch (this) {
                case PRECISION:
                    name = "Precision";
                    min = CatalogUtils.getMinPrecision(type);
                    max = CatalogUtils.getMaxPrecision(type);
                    break;
                case SCALE:
                    name = "Scale";
                    min = CatalogUtils.getMinScale(type);
                    max = CatalogUtils.getMaxScale(type);
                    break;
                case LENGTH:
                    name = "Length";
                    min = CatalogUtils.getMinLength(type);
                    max = CatalogUtils.getMaxLength(type);
                    break;
                default:
                    throw new IllegalArgumentException("Unexpected type parameter: " + this);
            }
            return format(ERR_COL_INVALID_TYPE_PARAM,
                    name,
                    column,
                    type,
                    value,
                    min,
                    max
            );
        }
    }
}
