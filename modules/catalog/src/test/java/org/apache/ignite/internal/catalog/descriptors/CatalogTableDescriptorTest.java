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

package org.apache.ignite.internal.catalog.descriptors;

import static org.apache.ignite.internal.catalog.CatalogManager.INITIAL_TIMESTAMP;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrows;
import static org.assertj.core.api.SoftAssertions.assertSoftly;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.startsWith;

import it.unimi.dsi.fastutil.ints.IntList;
import java.util.Collections;
import java.util.List;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.sql.ColumnType;
import org.junit.jupiter.api.Test;

class CatalogTableDescriptorTest {
    @Test
    void toStringContainsTypeAndFields() {
        List<CatalogTableColumnDescriptor> columns = List.of(
                new CatalogTableColumnDescriptor("pkCol", ColumnType.STRING, false, 0, 0, 10, null)
        );
        var descriptor = CatalogTableDescriptor.builder()
                .id(1)
                .schemaId(2)
                .primaryKeyIndexId(3)
                .name("table1")
                .zoneId(4)
                .newColumns(columns)
                .primaryKeyColumns(IntList.of(0))
                .storageProfile(CatalogService.DEFAULT_STORAGE_PROFILE)
                .build();

        String toString = descriptor.toString();

        assertThat(toString, startsWith("CatalogTableDescriptor ["));
        assertThat(toString, containsString("id=1"));
        assertThat(toString, containsString("schemaId=2"));
        assertThat(toString, containsString("pkIndexId=3"));
        assertThat(toString, containsString("name=table1"));
        assertThat(toString, containsString("zoneId=4"));
        assertThat(toString, not(containsString("schemaVersions=")));
    }

    @Test
    void itHasCorrectValues() {
        List<CatalogTableColumnDescriptor> columns = List.of(
                new CatalogTableColumnDescriptor("pkCol", ColumnType.STRING, false, 0, 0, 10, null)
        );
        var descriptor = CatalogTableDescriptor.builder()
                .id(1)
                .schemaId(2)
                .primaryKeyIndexId(3)
                .name("table1")
                .zoneId(4)
                .newColumns(columns)
                .primaryKeyColumns(IntList.of(0))
                .storageProfile(CatalogService.DEFAULT_STORAGE_PROFILE)
                .build();

        assertSoftly(assertions -> {
            assertions.assertThat(descriptor.colocationColumns().intStream()).containsExactly(0);
            assertions.assertThat(descriptor.updateTimestamp()).isEqualTo(INITIAL_TIMESTAMP);
            assertions.assertThat(descriptor.isPrimaryKeyColumn("pkCol")).isTrue();
            assertions.assertThat(descriptor.latestSchemaVersion()).isEqualTo(CatalogTableDescriptor.INITIAL_TABLE_VERSION);
            assertions.assertThat(descriptor.schemaVersions().latestVersion()).isEqualTo(CatalogTableDescriptor.INITIAL_TABLE_VERSION);
            assertions.assertThat(descriptor.storageProfile()).isEqualTo(CatalogService.DEFAULT_STORAGE_PROFILE);
        });

        var descriptorV2 = descriptor.copyBuilder()
                .newColumns(descriptor.columns())
                .timestamp(HybridTimestamp.MAX_VALUE)
                .build();

        assertSoftly(assertions -> {
            assertions.assertThat(descriptor.colocationColumns().intStream()).containsExactly(0);
            assertions.assertThat(descriptorV2.updateTimestamp()).isEqualTo(HybridTimestamp.MAX_VALUE);
            assertions.assertThat(descriptorV2.isPrimaryKeyColumn("pkCol")).isTrue();
            assertions.assertThat(descriptorV2.latestSchemaVersion()).isEqualTo(2);

            assertions.assertThat(descriptorV2.schemaVersions().latestVersion()).isEqualTo(2);
            assertions.assertThat(descriptorV2.storageProfile()).isEqualTo(CatalogService.DEFAULT_STORAGE_PROFILE);
        });
    }

    @Test
    @SuppressWarnings("ThrowableNotThrown")
    void descriptorValidatesArguments() {
        List<CatalogTableColumnDescriptor> columns = List.of(
                new CatalogTableColumnDescriptor("pkCol", ColumnType.STRING, false, 0, 0, 10, null)
        );
        CatalogTableDescriptor.Builder baseBuilder = CatalogTableDescriptor.builder()
                .id(1)
                .schemaId(2)
                .primaryKeyIndexId(3)
                .name("table1")
                .zoneId(4)
                .newColumns(columns)
                .primaryKeyColumns(IntList.of(0))
                .storageProfile(CatalogService.DEFAULT_STORAGE_PROFILE);

        assertThrows(IllegalArgumentException.class, () -> {
            baseBuilder
                    .newColumns(null)
                    .build();
        }, "Neither columns nor schemaVersions are defined.");

        assertThrows(IllegalArgumentException.class, () -> {
            baseBuilder
                    .newColumns(Collections.emptyList())
                    .build();
        }, "Neither columns nor schemaVersions are defined.");

        assertThrows(NullPointerException.class, () -> {
            baseBuilder
                    .newColumns(columns)
                    .primaryKeyColumns(null)
                    .build();
        }, "No primary key columns.");

        assertThrows(NullPointerException.class, () -> {
            baseBuilder
                    .primaryKeyColumns(IntList.of(0))
                    .storageProfile(null)
                    .build();
        }, "No storage profile.");
    }
}
