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

import static org.apache.ignite.sql.ColumnType.INT32;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import org.apache.ignite.internal.catalog.CatalogValidationException;
import org.apache.ignite.internal.catalog.descriptors.CatalogSystemViewDescriptor.SystemViewType;
import org.apache.ignite.internal.lang.RunnableX;
import org.apache.ignite.sql.ColumnType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests to verify validation of {@link CreateSystemViewCommand}.
 */
public class CreateSystemViewCommandValidationTest extends AbstractCommandValidationTest {

    @ParameterizedTest(name = "[{index}] ''{argumentsWithNames}''")
    @MethodSource("nullAndBlankStrings")
    public void nameShouldNotBeNullOrBlank(String name) {
        CreateSystemViewCommandBuilder builder = CreateSystemViewCommand.builder();

        builder = fillProperties(builder).name(name);

        expectValidationError(builder::build, "Name");
    }

    @ParameterizedTest(name = "[{index}] {argumentsWithNames}")
    @MethodSource("nullAndEmptyLists")
    void systemViewShouldHaveAtLeastOneColumn(List<ColumnParams> columns) {
        CreateSystemViewCommandBuilder builder = CreateSystemViewCommand.builder();

        builder = fillProperties(builder);

        builder = builder.columns(columns);

        expectValidationError(builder::build, "System view should have at least one column");
    }

    @Test
    void columnShouldNotHaveDuplicates() {
        CreateSystemViewCommandBuilder builder = CreateSystemViewCommand.builder();

        ColumnParams column = ColumnParams.builder()
                .name("C")
                .type(INT32)
                .build();

        fillProperties(builder);

        builder = builder.columns(List.of(column, column));

        expectValidationError(builder::build, "Column with name 'C' specified more than once");
    }

    @ParameterizedTest(name = "[{index}] ''{argumentsWithNames}''")
    @MethodSource("nullAndEmptyLists")
    public void columnsShouldNotBeNullNorEmpty(List<ColumnParams> columns) {
        CreateSystemViewCommandBuilder builder = CreateSystemViewCommand.builder();

        fillProperties(builder);

        builder = builder.columns(columns);

        expectValidationError(builder::build, "System view should have at least one column");
    }

    @Test
    public void typeShouldBeSpecified() {
        CreateSystemViewCommandBuilder builder = CreateSystemViewCommand.builder();

        builder = fillProperties(builder);

        builder = builder.type(null);

        expectValidationError(builder::build, "System view type is not specified");
    }

    private static void expectValidationError(RunnableX runnable, String message) {
        CatalogValidationException ex = assertThrows(CatalogValidationException.class, runnable::run);

        assertThat(ex.getMessage(), containsString(message));
    }

    private static CreateSystemViewCommandBuilder fillProperties(CreateSystemViewCommandBuilder builder) {
        ColumnParams column = ColumnParams.builder().name("C").type(ColumnType.INT8).build();
        List<ColumnParams> columns = List.of(column);

        return builder.name("view").columns(columns).type(SystemViewType.NODE);
    }
}
