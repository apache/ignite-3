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

package org.apache.ignite.internal.catalog.storage;

import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.hlc.HybridTimestamp;

/**
 * Random {@link CatalogTableDescriptor}s for testing.
 */
final class TestTableDescriptors {

    /**
     * Generates a list of {@link CatalogTableDescriptor}s of the given version.
     *
     * @param state Random state,
     * @param version Version.
     * @return A list of descriptors.
     */
    static List<CatalogTableDescriptor> tables(TestDescriptorState state, int version) {
        switch (version) {
            case 1:
            case 2:
                return tablesV0(state);
            default:
                throw new IllegalArgumentException("Unexpected table version: " + version);
        }
    }

    private static List<CatalogTableDescriptor> tablesV0(TestDescriptorState state) {
        List<CatalogTableDescriptor> list = new ArrayList<>();

        CatalogTableDescriptor table1 = CatalogTableDescriptor.builder()
                .id(state.id())
                .schemaId(state.id())
                .primaryKeyIndexId(state.id())
                .name(state.name("TABLE"))
                .zoneId(100)
                .columns(TestTableColumnDescriptors.columns(state))
                .primaryKeyColumns(List.of("C0"))
                .colocationColumns(null)
                .storageProfile("S1")
                .build();

        list.add(table1);
        list.add(table1.copyBuilder()
                .name(table1.name() + "_1")
                .columns(TestTableColumnDescriptors.columns(state).subList(0, 10))
                .timestamp(HybridTimestamp.hybridTimestamp(1232L))
                .storageProfile("S1")
                .latestSchemaVersion(2)
                .build()
        );
        list.add(
                list.get(list.size() - 1).copyBuilder()
                        .name(table1.name() + "_2")
                        .columns(TestTableColumnDescriptors.columns(state).subList(0, 20))
                        .timestamp(HybridTimestamp.hybridTimestamp(21232L))
                        .storageProfile("S1")
                        .latestSchemaVersion(3)
                        .build()
        );

        CatalogTableDescriptor table2 = CatalogTableDescriptor.builder()
                .id(state.id())
                .schemaId(state.id())
                .primaryKeyIndexId(state.id())
                .name(state.name("TABLE"))
                .zoneId(101)
                .columns(TestTableColumnDescriptors.columns(state))
                .primaryKeyColumns(List.of("C4"))
                .storageProfile("S2")
                .build();

        list.add(table2);
        list.add(table2.copyBuilder()
                .name(table2.name() + "_1")
                .columns(TestTableColumnDescriptors.columns(state).subList(0, 10))
                .timestamp(HybridTimestamp.hybridTimestamp(4567L))
                .storageProfile("S2")
                .latestSchemaVersion(2)
                .build()
        );
        list.add(list.get(list.size() - 1).copyBuilder()
                .name(table2.name() + "_2")
                .columns(TestTableColumnDescriptors.columns(state).subList(0, 20))
                .timestamp(HybridTimestamp.hybridTimestamp(8833L))
                .storageProfile("S2")
                .latestSchemaVersion(3)
                .build()
        );

        CatalogTableDescriptor table3 = CatalogTableDescriptor.builder()
                .id(state.id())
                .schemaId(state.id())
                .primaryKeyIndexId(state.id())
                .name(state.name("TABLE"))
                .zoneId(102)
                .columns(TestTableColumnDescriptors.columns(state))
                .primaryKeyColumns(List.of("C1", "C2", "C3"))
                .colocationColumns(List.of("C2", "C3"))
                .storageProfile("S3")
                .build();
        list.add(table3);
        list.add(table3.copyBuilder()
                .name(table3.name() + "_1")
                .columns(TestTableColumnDescriptors.columns(state))
                .timestamp(HybridTimestamp.hybridTimestamp(123234L))
                .storageProfile("S4")
                .latestSchemaVersion(2)
                .build()
        );

        return list;
    }
}
