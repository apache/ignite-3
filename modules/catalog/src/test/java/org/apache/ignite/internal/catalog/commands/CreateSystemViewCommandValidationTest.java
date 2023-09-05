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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import java.util.stream.Stream;
import org.apache.ignite.internal.catalog.CatalogValidationException;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.testframework.IgniteTestUtils.RunnableX;
import org.apache.ignite.sql.ColumnType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests to verify validation of {@link CreateSystemViewCommand}.
 */
public class CreateSystemViewCommandValidationTest extends BaseIgniteAbstractTest {

    private static Stream<Arguments> nullAndBlankStrings() {
        return Stream.of(null, "", " ", "  ").map(Arguments::of);
    }

    private static Stream<Arguments> nullAndEmptyLists() {
        return Stream.of(null, List.of()).map(Arguments::of);
    }

    @Test
    public void newSystemView() {
        CreateSystemViewCommand command = CreateSystemViewCommand.builder()
                .name("view")
                .columns(List.of(ColumnParams.builder().name("C1").type(INT32).build()))
                .build();

        assertEquals("view", command.name());

        ColumnParams column = command.columns().get(0);
        assertEquals("C1", column.name());
        assertEquals(INT32, column.type());

        assertEquals(1, command.columns().size(), "num columns");
    }

    @ParameterizedTest(name = "[{index}] ''{argumentsWithNames}''")
    @MethodSource("nullAndBlankStrings")
    public void nameShouldNotBeNullOrBlank(String name) {
        CreateSystemViewCommandBuilder builder = CreateSystemViewCommand.builder();

        builder = fillProperties(builder).name(name);

        expectValidationError(builder::build, "Name");
    }

    @ParameterizedTest(name = "[{index}] {argumentsWithNames}")
    @MethodSource("nullAndEmptyLists")
    void tableShouldHaveAtLeastOneColumn(List<ColumnParams> columns) {
        CreateSystemViewCommandBuilder builder = CreateSystemViewCommand.builder();

        builder = fillProperties(builder);

        builder.columns(columns);

        expectValidationError(builder::build, "System view should have at least one column");
    }

    @ParameterizedTest(name = "[{index}] ''{argumentsWithNames}''")
    @MethodSource("nullAndBlankStrings")
    void tableColumnNameMustNotBeNullOrBlank(String name) {
        CreateSystemViewCommandBuilder builder = CreateSystemViewCommand.builder();

        builder = fillProperties(builder);

        builder.columns(List.of(
                ColumnParams.builder().name(name).build()
        ));

        expectValidationError(builder::build, "Name of the column can't be null or blank");
    }

    @Test
    void tableColumnShouldHaveType() {
        CreateSystemViewCommandBuilder builder = CreateSystemViewCommand.builder();

        builder = fillProperties(builder)
                .columns(List.of(
                        ColumnParams.builder()
                                .name("C")
                                .type(null)
                                .build()
                ));

        expectValidationError(builder::build, "Missing column type: C");
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
    public void columnsNotNotBeNullNorEmpty(List<ColumnParams> columns) {
        CreateSystemViewCommandBuilder builder = CreateSystemViewCommand.builder();

        fillProperties(builder);

        expectValidationError(() -> builder.columns(columns).build(), "System view should have at least one column");
    }

    private static void expectValidationError(RunnableX runnable, String message) {
        CatalogValidationException ex = assertThrows(CatalogValidationException.class, runnable::run);

        assertThat(ex.getMessage(), containsString(message));
    }

    private static CreateSystemViewCommandBuilder fillProperties(CreateSystemViewCommandBuilder builder) {
        ColumnParams column = ColumnParams.builder().name("C").type(ColumnType.INT8).build();
        List<ColumnParams> columns = List.of(column);

        return builder.name("view").columns(columns);
    }
}
