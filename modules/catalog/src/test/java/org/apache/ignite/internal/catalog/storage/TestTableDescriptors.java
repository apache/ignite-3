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

import static org.apache.ignite.internal.hlc.HybridTimestamp.hybridTimestamp;

import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;

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
        list.add(list.get(0).newDescriptor(
                table1.name() + "_1", 2,
                TestTableColumnDescriptors.columns(state).subList(0, 10),
                hybridTimestamp(1232L),
                "S1")
        );
        list.add(
                list.get(list.size() - 1)
                        .newDescriptor(table1.name() + "_2", 3,
                                TestTableColumnDescriptors.columns(state).subList(0, 20), hybridTimestamp(21232L), "S1")
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
        list.add(list.get(list.size() - 1).newDescriptor(
                table2.name() + "_1", 2,
                TestTableColumnDescriptors.columns(state).subList(0, 10),
                hybridTimestamp(4567L), "S2")
        );
        list.add(list.get(list.size() - 1).newDescriptor(
                table2.name() + "_2", 3,
                TestTableColumnDescriptors.columns(state).subList(0, 20),
                hybridTimestamp(8833L),
                "S2")
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
        list.add(list.get(list.size() - 1).newDescriptor(
                table3.name() + "_1",
                2,
                TestTableColumnDescriptors.columns(state),
                hybridTimestamp(123234L),
                "S4")
        );

        return list;
    }
}
