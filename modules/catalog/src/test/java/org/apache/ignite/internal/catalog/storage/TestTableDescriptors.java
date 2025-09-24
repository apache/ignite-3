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
import org.apache.ignite.internal.catalog.descriptors.CatalogTableColumnDescriptor;
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

        CatalogTableDescriptor table1 = new CatalogTableDescriptor(
                state.id(),
                state.id(),
                state.id(),
                state.name("TABLE"),
                100,
                TestTableColumnDescriptors.columns(state),
                List.of("C0"),
                null,
                "S1"
        );

        list.add(table1);
        String name4 = table1.name() + "_1";
        List<CatalogTableColumnDescriptor> columns4 = TestTableColumnDescriptors.columns(state).subList(0, 10);
        list.add(list.get(0).copyBuilder()
                .name(name4)
                .columns(columns4)
                .timestamp(HybridTimestamp.hybridTimestamp(1232L))
                .storageProfile("S1")
                .tableVersion(2)
                .build()
        );
        String name3 = table1.name() + "_2";
        List<CatalogTableColumnDescriptor> columns3 = TestTableColumnDescriptors.columns(state).subList(0, 20);
        list.add(
                list.get(list.size() - 1).copyBuilder()
                        .name(name3)
                        .columns(columns3)
                        .timestamp(HybridTimestamp.hybridTimestamp(21232L))
                        .storageProfile("S1")
                        .tableVersion(3)
                        .build()
        );

        CatalogTableDescriptor table2 = new CatalogTableDescriptor(
                state.id(),
                state.id(),
                state.id(),
                state.name("TABLE"),
                101,
                TestTableColumnDescriptors.columns(state), List.of("C4"), null,
                "S2"
        );

        list.add(table2);
        String name2 = table2.name() + "_1";
        List<CatalogTableColumnDescriptor> columns2 = TestTableColumnDescriptors.columns(state).subList(0, 10);
        list.add(list.get(list.size() - 1).copyBuilder()
                .name(name2)
                .columns(columns2)
                .timestamp(HybridTimestamp.hybridTimestamp(4567L))
                .storageProfile("S2")
                .tableVersion(2)
                .build()
        );
        String name1 = table2.name() + "_2";
        List<CatalogTableColumnDescriptor> columns1 = TestTableColumnDescriptors.columns(state).subList(0, 20);
        list.add(list.get(list.size() - 1).copyBuilder()
                .name(name1)
                .columns(columns1)
                .timestamp(HybridTimestamp.hybridTimestamp(8833L))
                .storageProfile("S2")
                .tableVersion(3)
                .build()
        );

        CatalogTableDescriptor table3 = new CatalogTableDescriptor(
                state.id(),
                state.id(),
                state.id(),
                state.name("TABLE"),
                102,
                TestTableColumnDescriptors.columns(state),
                List.of("C1", "C2", "C3"),
                List.of("C2", "C3"),
                "S3"
        );
        list.add(table3);
        String name = table3.name() + "_1";
        List<CatalogTableColumnDescriptor> columns = TestTableColumnDescriptors.columns(state);
        list.add(list.get(list.size() - 1).copyBuilder()
                .name(name)
                .columns(columns)
                .timestamp(HybridTimestamp.hybridTimestamp(123234L))
                .storageProfile("S4")
                .tableVersion(2)
                .build()
        );

        return list;
    }
}
