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

import static org.apache.ignite.internal.catalog.CatalogTestUtils.createCatalogManagerWithTestUpdateLog;
import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.ForkJoinPool;
import org.apache.ignite.internal.catalog.commands.ColumnParams;
import org.apache.ignite.internal.catalog.commands.CreateTableCommand;
import org.apache.ignite.internal.catalog.commands.CreateTableCommandBuilder;
import org.apache.ignite.internal.catalog.commands.TableHashPrimaryKey;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.sql.ColumnType;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.junit.jupiter.api.Test;

/** Tests to verify {@link CatalogTestUtils}. */
class CatalogTestUtilsTest extends BaseIgniteAbstractTest {
    /**
     * Simple smoke test to verify test manager is able to process several versions of catalog,
     * and returned instance follows the contract.
     */
    @Test
    void testManagerWorksAsExpected() {
        CatalogManager manager = createCatalogManagerWithTestUpdateLog("test", new HybridClockImpl());

        assertThat(manager.startAsync(ForkJoinPool.commonPool()), willCompleteSuccessfully());

        CreateTableCommandBuilder createTableTemplate = CreateTableCommand.builder()
                .schemaName("PUBLIC")
                .columns(List.of(
                        ColumnParams.builder().name("C1").type(ColumnType.INT32).build(),
                        ColumnParams.builder().name("C2").type(ColumnType.INT32).build()
                ))
                .primaryKey(TableHashPrimaryKey.builder()
                        .columns(List.of("C1"))
                        .build());

        assertThat(manager.execute(createTableTemplate.tableName("T1").build()), willCompleteSuccessfully());

        int version1 = manager.latestCatalogVersion();

        assertThat(manager.execute(createTableTemplate.tableName("T2").build()), willCompleteSuccessfully());

        int version2 = manager.latestCatalogVersion();

        Collection<CatalogTableDescriptor> tablesOfVersion1 = manager.tables(version1);

        assertThat(tablesOfVersion1, hasSize(1));
        assertThat(tablesOfVersion1, hasItem(descriptorWithName("T1")));

        Collection<CatalogTableDescriptor> tablesOfVersion2 = manager.tables(version2);

        assertThat(tablesOfVersion2, hasSize(2));
        assertThat(tablesOfVersion2, hasItem(descriptorWithName("T1")));
        assertThat(tablesOfVersion2, hasItem(descriptorWithName("T2")));

        assertThat(manager.stopAsync(), willCompleteSuccessfully());
    }

    private static Matcher<CatalogTableDescriptor> descriptorWithName(String name) {
        return new BaseMatcher<>() {
            @Override
            public boolean matches(Object actual) {
                return actual instanceof CatalogTableDescriptor && name.equals(((CatalogTableDescriptor) actual).name());
            }

            @Override
            public void describeTo(Description description) {
                description.appendText(format("should have name '{}'", name));
            }
        };
    }
}
