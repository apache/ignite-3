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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.ignite.internal.catalog.descriptors.CatalogColumnCollation;
import org.apache.ignite.internal.catalog.descriptors.CatalogHashIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexColumnDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexStatus;
import org.apache.ignite.internal.catalog.descriptors.CatalogSortedIndexDescriptor;

/**
 * Random {@link CatalogIndexDescriptor}s for testing.
 */
final class TestIndexDescriptors {

    private static CatalogIndexColumnDescriptor column(String name, CatalogColumnCollation collation) {
        //noinspection removal
        return new CatalogIndexColumnDescriptor(name, collation);
    }

    private static CatalogIndexColumnDescriptor column(int columnId, CatalogColumnCollation collation) {
        return new CatalogIndexColumnDescriptor(columnId, collation);
    }

    /**
     * Generates a list of {@link CatalogSortedIndexDescriptor}s of the given version.
     *
     * @param state Random state,
     * @param version Version.l
     * @return A list of descriptors.
     */
    static List<CatalogSortedIndexDescriptor> sortedIndices(TestDescriptorState state, int version) {
        switch (version) {
            case 1:
            case 2:
                return sortedIndicesV0(state);
            case 3:
                return sortedIndicesV3(state);
            default:
                throw new IllegalArgumentException("Unexpected CatalogSortedIndexDescriptor version: " + version);
        }
    }

    private static List<CatalogSortedIndexDescriptor> sortedIndicesV0(TestDescriptorState state) {
        List<CatalogSortedIndexDescriptor> list = new ArrayList<>();

        for (var unique : new boolean[]{true, false}) {
            for (var indexStatus : CatalogIndexStatus.values()) {
                for (var isCreatedWithTable : new boolean[]{false, true}) {
                    List<CatalogIndexColumnDescriptor> columns = Arrays.asList(
                            column("C1", CatalogColumnCollation.ASC_NULLS_FIRST),
                            column("C2", CatalogColumnCollation.ASC_NULLS_LAST),
                            column("C3", CatalogColumnCollation.DESC_NULLS_FIRST),
                            column("C4", CatalogColumnCollation.DESC_NULLS_LAST)
                    );
                    Collections.shuffle(columns, state.random());

                    list.add(new CatalogSortedIndexDescriptor(
                            state.id(),
                            state.name("SORTED_IDX"),
                            1000 + list.size(),
                            unique,
                            indexStatus,
                            columns,
                            isCreatedWithTable));
                }
            }
        }

        return list;
    }

    private static List<CatalogSortedIndexDescriptor> sortedIndicesV3(TestDescriptorState state) {
        List<CatalogSortedIndexDescriptor> list = new ArrayList<>();

        for (var unique : new boolean[]{true, false}) {
            for (var indexStatus : CatalogIndexStatus.values()) {
                for (var isCreatedWithTable : new boolean[]{false, true}) {
                    List<CatalogIndexColumnDescriptor> columns = Arrays.asList(
                            column(0, CatalogColumnCollation.ASC_NULLS_FIRST),
                            column(1, CatalogColumnCollation.ASC_NULLS_LAST),
                            column(2, CatalogColumnCollation.DESC_NULLS_FIRST),
                            column(3, CatalogColumnCollation.DESC_NULLS_LAST)
                    );
                    Collections.shuffle(columns, state.random());

                    list.add(new CatalogSortedIndexDescriptor(
                            state.id(),
                            state.name("SORTED_IDX"),
                            1000 + list.size(),
                            unique,
                            indexStatus,
                            columns,
                            isCreatedWithTable));
                }
            }
        }

        return list;
    }

    static List<CatalogHashIndexDescriptor> hashIndices(TestDescriptorState state, int version) {
        switch (version) {
            case 1:
            case 2:
                return hashIndicesV0(state);
            case 3:
                return hashIndicesV3(state);
            default:
                throw new IllegalArgumentException("Unexpected CatalogSortedIndexDescriptor version: " + version);
        }
    }

    private static List<CatalogHashIndexDescriptor> hashIndicesV0(TestDescriptorState state) {
        List<CatalogHashIndexDescriptor> list = new ArrayList<>();

        for (var unique : new boolean[]{true, false}) {
            for (var indexStatus : CatalogIndexStatus.values()) {
                for (var isCreatedWithTable : new boolean[]{false, true}) {
                    List<String> columns = Arrays.asList("C1", "C2");
                    Collections.shuffle(columns, state.random());

                    //noinspection removal
                    list.add(new CatalogHashIndexDescriptor(
                            state.id(),
                            state.name("HASH_IDX"),
                            1000 + list.size(),
                            unique,
                            indexStatus,
                            columns,
                            isCreatedWithTable));
                }
            }
        }

        return list;
    }

    private static List<CatalogHashIndexDescriptor> hashIndicesV3(TestDescriptorState state) {
        List<CatalogHashIndexDescriptor> list = new ArrayList<>();

        for (var unique : new boolean[]{true, false}) {
            for (var indexStatus : CatalogIndexStatus.values()) {
                for (var isCreatedWithTable : new boolean[]{false, true}) {
                    IntList columns = IntList.of(0, 1);

                    list.add(new CatalogHashIndexDescriptor(
                            state.id(),
                            state.name("HASH_IDX"),
                            1000 + list.size(),
                            unique,
                            indexStatus,
                            columns,
                            isCreatedWithTable));
                }
            }
        }

        return list;
    }
}
