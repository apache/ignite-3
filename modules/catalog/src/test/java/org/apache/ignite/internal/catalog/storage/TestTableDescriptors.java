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

import it.unimi.dsi.fastutil.ints.IntList;
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
            case 3:
                return tablesV3(state);
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
                .newColumns(TestTableColumnDescriptors.columns(state))
                .primaryKeyColumns(IntList.of(0))
                .colocationColumns(null)
                .storageProfile("S1")
                .build();

        list.add(table1);
        list.add(table1.copyBuilder()
                .name(table1.name() + "_1")
                .newColumns(TestTableColumnDescriptors.columns(state).subList(0, 10))
                .timestamp(HybridTimestamp.hybridTimestamp(1232L))
                .storageProfile("S1")
                .build()
        );
        list.add(
                list.get(list.size() - 1).copyBuilder()
                        .name(table1.name() + "_2")
                        .newColumns(TestTableColumnDescriptors.columns(state).subList(0, 20))
                        .timestamp(HybridTimestamp.hybridTimestamp(21232L))
                        .storageProfile("S1")
                        .build()
        );

        CatalogTableDescriptor table2 = CatalogTableDescriptor.builder()
                .id(state.id())
                .schemaId(state.id())
                .primaryKeyIndexId(state.id())
                .name(state.name("TABLE"))
                .zoneId(101)
                .newColumns(TestTableColumnDescriptors.columns(state))
                .primaryKeyColumns(IntList.of(4))
                .storageProfile("S2")
                .build();

        list.add(table2);
        list.add(table2.copyBuilder()
                .name(table2.name() + "_1")
                .newColumns(TestTableColumnDescriptors.columns(state).subList(0, 10))
                .timestamp(HybridTimestamp.hybridTimestamp(4567L))
                .storageProfile("S2")
                .build()
        );
        list.add(list.get(list.size() - 1).copyBuilder()
                .name(table2.name() + "_2")
                .newColumns(TestTableColumnDescriptors.columns(state).subList(0, 20))
                .timestamp(HybridTimestamp.hybridTimestamp(8833L))
                .build()
        );

        CatalogTableDescriptor table3 = CatalogTableDescriptor.builder()
                .id(state.id())
                .schemaId(state.id())
                .primaryKeyIndexId(state.id())
                .name(state.name("TABLE"))
                .zoneId(102)
                .newColumns(TestTableColumnDescriptors.columns(state))
                .primaryKeyColumns(IntList.of(1, 2, 3))
                .colocationColumns(IntList.of(2, 3))
                .storageProfile("S3")
                .build();
        list.add(table3);
        list.add(table3.copyBuilder()
                .name(table3.name() + "_1")
                .newColumns(TestTableColumnDescriptors.columns(state))
                .timestamp(HybridTimestamp.hybridTimestamp(123234L))
                .storageProfile("S4")
                .build()
        );

        return list;
    }

    private static List<CatalogTableDescriptor> tablesV3(TestDescriptorState state) {
        List<CatalogTableDescriptor> tables = new ArrayList<>(tablesV0(state));

        tables.add(CatalogTableDescriptor.builder()
                .id(state.id())
                .schemaId(state.id())
                .primaryKeyIndexId(state.id())
                .name(state.name("TABLE"))
                .zoneId(101)
                .newColumns(TestTableColumnDescriptors.columns(state))
                .primaryKeyColumns(IntList.of(4))
                .storageProfile("S2")
                .staleRowsFraction(0.3d)
                .minStaleRowsCount(state.id() * 10_000L)
                .build()
        );

        return tables;
    }
}
