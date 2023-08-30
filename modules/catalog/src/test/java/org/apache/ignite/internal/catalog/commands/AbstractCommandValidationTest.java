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
import org.apache.ignite.internal.catalog.ClockWaiter;
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
import org.junit.jupiter.params.provider.Arguments;

/**
 * Abstract test class for all command-related validation tests.
 *
 * <p>Provides a convenient set of utility methods to reuse in test.
 */
abstract class AbstractCommandValidationTest extends BaseIgniteAbstractTest {
    static final String SCHEMA_NAME = "PUBLIC";
    static final String ZONE_NAME = "DEFAULT";

    private static final CatalogZoneDescriptor DEFAULT_ZONE = new CatalogZoneDescriptor(
            0, ZONE_NAME, 1, -1, -1, -1, -1, "", null
    );

    final CatalogManager manager = new CatalogManagerImpl(
            mock(UpdateLog.class),
            mock(ClockWaiter.class)
    );

    static Stream<Arguments> nullAndBlankStrings() {
        return Stream.of(null, "", " ", "  ").map(Arguments::of);
    }

    static Stream<Arguments> nullAndEmptyLists() {
        return Stream.of(null, List.of()).map(Arguments::of);
    }

    static <T extends Throwable> void assertThrows(RunnableX runnable, Class<T> expectedType, String message) {
        T ex = Assertions.assertThrows(
                expectedType,
                runnable::run
        );

        assertThat(
                ex.getMessage(),
                Matchers.containsString(message)
        );
    }

    static Catalog emptyCatalog() {
        return catalog(new CatalogTableDescriptor[0], new CatalogIndexDescriptor[0]);
    }

    static Catalog catalogWithTable(String name) {
        int tableId = 0;
        int tableVersion = 1;
        int zoneId = 0;
        List<CatalogTableColumnDescriptor> columns = List.of(
                new CatalogTableColumnDescriptor("C", INT32, false, -1, -1, -1, null)
        );
        List<String> pkColumns = List.of("C");

        CatalogTableDescriptor table = new CatalogTableDescriptor(
                tableId, name, zoneId, tableVersion, columns, pkColumns, pkColumns
        );

        return catalog(new CatalogTableDescriptor[]{table}, new CatalogIndexDescriptor[0]);
    }

    static Catalog catalogWithIndex(String name) {
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
