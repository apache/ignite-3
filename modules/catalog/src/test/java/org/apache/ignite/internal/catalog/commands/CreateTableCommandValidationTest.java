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
import static org.mockito.Mockito.mock;

import java.util.List;
import java.util.stream.Stream;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.catalog.CatalogManagerImpl;
import org.apache.ignite.internal.catalog.CatalogValidationException;
import org.apache.ignite.internal.catalog.ClockWaiter;
import org.apache.ignite.internal.catalog.UpdateProducer;
import org.apache.ignite.internal.catalog.descriptors.CatalogHashIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogSchemaDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableColumnDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.catalog.storage.UpdateLog;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.testframework.IgniteTestUtils.RunnableX;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests to verify validation of {@link CreateTableCommand}.
 */
@SuppressWarnings("DataFlowIssue")
public class CreateTableCommandValidationTest extends BaseIgniteAbstractTest {
    private static final String SCHEMA_NAME = "PUBLIC";
    private static final String ZONE_NAME = "DEFAULT";

    private static final CatalogZoneDescriptor DEFAULT_ZONE = new CatalogZoneDescriptor(
            0, ZONE_NAME, 1, -1, -1, -1, -1, "", null
    );

    private final CatalogManager manager = new CatalogManagerImpl(
            mock(UpdateLog.class),
            mock(ClockWaiter.class)
    );

    private static Stream<Arguments> nullAndBlankStrings() {
        return Stream.of(null, "", " ", "  ").map(Arguments::of);
    }

    private static Stream<Arguments> nullAndEmptyLists() {
        return Stream.of(null, List.of()).map(Arguments::of);
    }

    @ParameterizedTest(name = "[{index}] ''{argumentsWithNames}''")
    @MethodSource("nullAndBlankStrings")
    void schemaNameMustNotBeNullOrBlank(String name) {
        CreateTableCommandBuilder builder = manager.createTableCommandBuilder();

        builder = fillProperties(builder);

        builder.schemaName(name);

        assertThrows(
                builder::build,
                CatalogValidationException.class,
                "Name of the schema can't be null or blank"
        );
    }

    @ParameterizedTest(name = "[{index}] ''{argumentsWithNames}''")
    @MethodSource("nullAndBlankStrings")
    void tableNameMustNotBeNullOrBlank(String name) {
        CreateTableCommandBuilder builder = manager.createTableCommandBuilder();

        builder = fillProperties(builder);

        builder.tableName(name);

        assertThrows(
                builder::build,
                CatalogValidationException.class,
                "Name of the table can't be null or blank"
        );
    }

    @ParameterizedTest(name = "[{index}] {argumentsWithNames}")
    @MethodSource("nullAndEmptyLists")
    void tableShouldHaveAtLeastOneColumn(List<ColumnParams> columns) {
        CreateTableCommandBuilder builder = manager.createTableCommandBuilder();

        builder = fillProperties(builder);

        builder.columns(columns);

        assertThrows(
                builder::build,
                CatalogValidationException.class,
                "Table should have at least one column"
        );
    }

    @ParameterizedTest(name = "[{index}] ''{argumentsWithNames}''")
    @MethodSource("nullAndBlankStrings")
    void tableColumnNameMustNotBeNullOrBlank(String name) {
        CreateTableCommandBuilder builder = manager.createTableCommandBuilder();

        builder = fillProperties(builder);

        builder.columns(List.of(
                ColumnParams.builder().name(name).build()
        ));

        assertThrows(
                builder::build,
                CatalogValidationException.class,
                "Name of the column can't be null or blank"
        );
    }

    @Test
    void tableColumnShouldHaveType() {
        CreateTableCommandBuilder builder = manager.createTableCommandBuilder();

        builder = fillProperties(builder)
                .columns(List.of(
                        ColumnParams.builder()
                                .name("C")
                                .type(null)
                                .build()
                ));

        assertThrows(
                builder::build,
                CatalogValidationException.class,
                "Missing column type: C"
        );
    }

    @Test
    void columnShouldNotHaveDuplicates() {
        CreateTableCommandBuilder builder = manager.createTableCommandBuilder();

        ColumnParams column = ColumnParams.builder()
                .name("C")
                .type(INT32)
                .build();

        builder = fillProperties(builder).columns(List.of(column, column));

        assertThrows(
                builder::build,
                CatalogValidationException.class,
                "Column with name 'C' specified more than once"
        );
    }

    @ParameterizedTest(name = "[{index}] {argumentsWithNames}")
    @MethodSource("nullAndEmptyLists")
    void tableShouldHaveAtLeastOnePrimaryKeyColumn(List<String> columns) {
        CreateTableCommandBuilder builder = manager.createTableCommandBuilder();

        builder = fillProperties(builder);

        builder.primaryKeyColumns(columns);

        assertThrows(
                builder::build,
                CatalogValidationException.class,
                "Table should have primary key"
        );
    }

    @Test
    void pkColumnShouldNotHaveDuplicates() {
        CreateTableCommandBuilder builder = manager.createTableCommandBuilder();

        builder = fillProperties(builder)
                .primaryKeyColumns(List.of("C", "C"));

        assertThrows(
                builder::build,
                CatalogValidationException.class,
                "PK column 'C' specified more that once"
        );
    }

    @Test
    void pkColumnShouldBePresentedInColumnsList() {
        CreateTableCommandBuilder builder = manager.createTableCommandBuilder();

        builder = fillProperties(builder)
                .primaryKeyColumns(List.of("foo"));

        assertThrows(
                builder::build,
                CatalogValidationException.class,
                "PK column 'foo' is not part of table"
        );
    }

    @Test
    void colocationColumnsCouldNotBeEmpty() {
        CreateTableCommandBuilder builder = manager.createTableCommandBuilder();

        builder = fillProperties(builder)
                .colocationColumns(List.of());

        assertThrows(
                builder::build,
                CatalogValidationException.class,
                "Colocation columns could not be empty"
        );
    }

    @Test
    void colocationColumnShouldNotHaveDuplicates() {
        CreateTableCommandBuilder builder = manager.createTableCommandBuilder();

        builder = fillProperties(builder)
                .colocationColumns(List.of("C", "C"));

        assertThrows(
                builder::build,
                CatalogValidationException.class,
                "Colocation column 'C' specified more that once"
        );
    }

    @Test
    void colocationColumnShouldBePresentedInColumnsList() {
        CreateTableCommandBuilder builder = manager.createTableCommandBuilder();

        ColumnParams c1 = ColumnParams.builder()
                .name("C1")
                .type(INT32)
                .build();

        ColumnParams c2 = ColumnParams.builder()
                .name("C2")
                .type(INT32)
                .build();

        builder = fillProperties(builder)
                .columns(List.of(c1, c2))
                .primaryKeyColumns(List.of("C1"))
                .colocationColumns(List.of("C2"));

        assertThrows(
                builder::build,
                CatalogValidationException.class,
                "Colocation column 'C2' is not part of PK"
        );
    }

    private static CreateTableCommandBuilder fillProperties(CreateTableCommandBuilder builder) {
        return builder
                .schemaName(SCHEMA_NAME)
                .tableName("TEST")
                .zone(ZONE_NAME)
                .columns(List.of(
                        ColumnParams.builder()
                                .name("C")
                                .type(INT32)
                                .build()
                ))
                .primaryKeyColumns(List.of("C"))
                .colocationColumns(List.of("C"));
    }

    @Test
    void exceptionIsThrownIfSchemaNotExists() {
        CreateTableCommandBuilder builder = manager.createTableCommandBuilder();

        Catalog catalog = emptyCatalog();

        UpdateProducer updateProducer = (UpdateProducer) fillProperties(builder).schemaName(SCHEMA_NAME + "_UNK").build();

        assertThrows(
                () -> updateProducer.get(catalog),
                CatalogValidationException.class,
                "Schema with name 'PUBLIC_UNK' not found"
        );
    }

    @Test
    void exceptionIsThrownIfZoneNotExists() {
        CreateTableCommandBuilder builder = manager.createTableCommandBuilder();

        Catalog catalog = emptyCatalog();

        UpdateProducer updateProducer = (UpdateProducer) fillProperties(builder).zone(ZONE_NAME + "_UNK").build();

        assertThrows(
                () -> updateProducer.get(catalog),
                CatalogValidationException.class,
                "Distribution zone with name 'DEFAULT_UNK' not found"
        );
    }

    @Test
    void exceptionIsThrownIfTableWithGivenNameAlreadyExists() {
        CreateTableCommandBuilder builder = manager.createTableCommandBuilder();

        Catalog catalog = catalogWithTable("TEST");

        UpdateProducer updateProducer = (UpdateProducer) fillProperties(builder).tableName("TEST").build();

        assertThrows(
                () -> updateProducer.get(catalog),
                CatalogValidationException.class,
                "Table with name 'PUBLIC.TEST' already exists"
        );
    }

    @Test
    void exceptionIsThrownIfIndexWithGivenNameAlreadyExists() {
        CreateTableCommandBuilder builder = manager.createTableCommandBuilder();

        Catalog catalog = catalogWithIndex("TEST");

        UpdateProducer updateProducer = (UpdateProducer) fillProperties(builder).tableName("TEST").build();

        assertThrows(
                () -> updateProducer.get(catalog),
                CatalogValidationException.class,
                "Index with name 'PUBLIC.TEST' already exists"
        );
    }

    @Test
    void exceptionIsThrownIfTableWithNameSimilarToAutogeneratedPkNameAlreadyExists() {
        CreateTableCommandBuilder builder = manager.createTableCommandBuilder();

        Catalog catalog = catalogWithTable("TEST_PK");

        UpdateProducer updateProducer = (UpdateProducer) fillProperties(builder).tableName("TEST").build();

        assertThrows(
                () -> updateProducer.get(catalog),
                CatalogValidationException.class,
                "Table with name 'PUBLIC.TEST_PK' already exists"
        );
    }

    @Test
    void exceptionIsThrownIfIndexWithNameSimilarToAutogeneratedPkNameAlreadyExists() {
        CreateTableCommandBuilder builder = manager.createTableCommandBuilder();

        Catalog catalog = catalogWithIndex("TEST_PK");

        UpdateProducer updateProducer = (UpdateProducer) fillProperties(builder).tableName("TEST").build();

        assertThrows(
                () -> updateProducer.get(catalog),
                CatalogValidationException.class,
                "Index with name 'PUBLIC.TEST_PK' already exists"
        );
    }

    private static <T extends Throwable> void assertThrows(RunnableX runnable, Class<T> expectedType, String message) {
        T ex = Assertions.assertThrows(
                expectedType,
                runnable::run
        );

        assertThat(
                ex.getMessage(),
                Matchers.containsString(message)
        );
    }

    private static Catalog emptyCatalog() {
        return catalog(new CatalogTableDescriptor[0], new CatalogIndexDescriptor[0]);
    }

    private static Catalog catalogWithTable(String name) {
        CatalogTableDescriptor table = new CatalogTableDescriptor(
                0, name, 0, 1, List.of(
                        new CatalogTableColumnDescriptor("C", INT32, false, -1, -1, -1, null)
        ), List.of("C"), List.of("C"));

        return catalog(new CatalogTableDescriptor[]{table}, new CatalogIndexDescriptor[0]);
    }

    private static Catalog catalogWithIndex(String name) {
        CatalogIndexDescriptor index = new CatalogHashIndexDescriptor(
                0, name, 0, false, List.of("C"));

        return catalog(new CatalogTableDescriptor[0], new CatalogIndexDescriptor[]{index});
    }

    private static Catalog catalog(CatalogTableDescriptor[] tables, CatalogIndexDescriptor[] indexes) {
        return new Catalog(
                1,
                0L,
                1,
                List.of(DEFAULT_ZONE),
                List.of(new CatalogSchemaDescriptor(
                        0,
                        SCHEMA_NAME,
                        tables,
                        indexes
                ))
        );
    }
}
