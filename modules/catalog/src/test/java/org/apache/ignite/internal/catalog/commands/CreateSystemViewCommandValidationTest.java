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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.apache.ignite.internal.catalog.CatalogValidationException;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
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
                .columns(List.of(ColumnParams.builder().name("C1").type(ColumnType.INT32).build()))
                .build();

        assertEquals("view", command.name());

        ColumnParams column = command.columns().get(0);
        assertEquals("C1", column.name());
        assertEquals(ColumnType.INT32, column.type());

        assertEquals(1, command.columns().size(), "num columns");
    }

    @ParameterizedTest(name = "[{index}] ''{argumentsWithNames}''")
    @MethodSource("nullAndBlankStrings")
    public void nameShouldNotBeNullOrBlank(String name) {
        expectThrows(() -> systemView().name(name).build(), "Name");
    }

    @ParameterizedTest(name = "[{index}] ''{argumentsWithNames}''")
    @MethodSource("nullAndEmptyLists")
    public void columnsNotNotBeNullNorEmpty(List<ColumnParams> columns) {
        expectThrows(() -> systemView().columns(columns).build(), "System view should have at least one column");
    }

    private static CreateSystemViewCommandBuilder systemView() {
        return CreateSystemViewCommand.builder()
                .name("view")
                .columns(List.of(ColumnParams.builder().name("C").type(ColumnType.INT8).build()));
    }

    private static void expectThrows(Supplier<CreateSystemViewCommand> builder, String errorMessage) {
        CatalogValidationException t = assertThrows(CatalogValidationException.class, builder::get);
        assertThat("error message", t.getMessage(), containsString(errorMessage));
    }
}
