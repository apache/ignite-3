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

import static org.apache.ignite.internal.catalog.BaseCatalogManagerTest.columnParamsBuilder;
import static org.apache.ignite.internal.catalog.CatalogTestUtils.DEFAULT_NULLABLE;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrowsWithCause;

import org.apache.ignite.internal.catalog.commands.ColumnParams;
import org.apache.ignite.internal.catalog.commands.ColumnParams.Builder;
import org.apache.ignite.sql.ColumnType;
import org.apache.ignite.sql.ColumnType.PrecisionScale;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.EnumSource.Mode;

/** Test {@link ColumnParams} assembly and validation. */
public class ColumnConstructionValidatorTest {
    @ParameterizedTest
    @EnumSource(value = ColumnType.class, names = "NULL", mode = Mode.EXCLUDE)
    public void testColumnSetPrecisionScale(ColumnType type) {
        Builder colBuilder = columnParamsBuilder("COL", type, DEFAULT_NULLABLE, 20);

        Builder colBuilderErrPrec1 = columnParamsBuilder("COL", type, DEFAULT_NULLABLE, Integer.MIN_VALUE, 2);
        Builder colBuilderErrPrec2 = columnParamsBuilder("COL", type, DEFAULT_NULLABLE, Integer.MAX_VALUE, 2);
        Builder colBuilderErrScale1 = columnParamsBuilder("COL", type, DEFAULT_NULLABLE, 20, Integer.MAX_VALUE);

        if (type.precScale() == PrecisionScale.NO_NO) {
            assertThrowsWithCause(colBuilder::build, CatalogValidationException.class, "Precision is not applicable for column of type");
        }

        if (type.precScale() == PrecisionScale.YES_NO) {
            Builder colBuilder0 = columnParamsBuilder("COL", type, DEFAULT_NULLABLE, 20, 2);
            assertThrowsWithCause(colBuilder0::build, CatalogValidationException.class, "Scale is not applicable for column of type");
        }

        if (type.precScale() == PrecisionScale.YES_NO || type.precScale() == PrecisionScale.YES_YES) {
            assertThrowsWithCause(colBuilderErrPrec1::build, CatalogValidationException.class, "must be between");
            assertThrowsWithCause(colBuilderErrPrec2::build, CatalogValidationException.class, "must be between");
        }

        if (type.precScale() == PrecisionScale.YES_YES) {
            assertThrowsWithCause(colBuilderErrScale1::build, CatalogValidationException.class, "must be between");
        }
    }
}
