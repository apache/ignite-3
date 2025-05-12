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
import java.util.Map;
import java.util.stream.Stream;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogObjectDescriptor.Type;
import org.apache.ignite.internal.catalog.descriptors.CatalogSchemaDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogSystemViewDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;

/**
 * Random {@link CatalogSchemaDescriptor}s for testing.
 */
final class TestSchemaDescriptors {

    /**
     * Generates a list of {@link CatalogSchemaDescriptor}s of the given version.
     *
     * @param state Random state,
     * @param version Version.
     * @param objects Versions of child objects. If not specified the {@code version} is used.
     * @return A list of descriptors.
     */
    static List<CatalogSchemaDescriptor> schemas(TestDescriptorState state, int version, Map<Type, Integer> objects) {
        switch (version) {
            case 1:
            case 2:
                return schemasV0(state, version, objects);
            default:
                throw new IllegalArgumentException("Unexpected CatalogSchemaDescriptor version: " + version);
        }
    }

    private static List<CatalogSchemaDescriptor> schemasV0(TestDescriptorState state, int version, Map<Type, Integer> objects) {
        List<CatalogSchemaDescriptor> list = new ArrayList<>();

        int tableVersion = objects.getOrDefault(Type.TABLE, version);

        CatalogTableDescriptor[] tables = TestTableDescriptors.tables(state, tableVersion)
                .toArray(new CatalogTableDescriptor[0]);

        int indexVersion = objects.getOrDefault(Type.INDEX, version);

        CatalogIndexDescriptor[] indexes = Stream.concat(
                        TestIndexDescriptors.hashIndices(state, indexVersion).stream(),
                        TestIndexDescriptors.sortedIndices(state, indexVersion).stream())
                .toArray(CatalogIndexDescriptor[]::new);

        int systemVersion = objects.getOrDefault(Type.SYSTEM_VIEW, version);

        CatalogSystemViewDescriptor[] systemViews = TestSystemViewDescriptors.systemViews(state, systemVersion)
                .toArray(new CatalogSystemViewDescriptor[0]);

        list.add(new CatalogSchemaDescriptor(
                state.id(),
                state.name("schema"),
                new CatalogTableDescriptor[0],
                new CatalogIndexDescriptor[0],
                new CatalogSystemViewDescriptor[0],
                hybridTimestamp(123232L))
        );

        list.add(new CatalogSchemaDescriptor(
                state.id(),
                state.name("schema"),
                tables,
                new CatalogIndexDescriptor[0],
                new CatalogSystemViewDescriptor[0],
                hybridTimestamp(76765754L))
        );

        list.add(new CatalogSchemaDescriptor(
                state.id(),
                state.name("schema"),
                tables,
                indexes,
                new CatalogSystemViewDescriptor[0],
                hybridTimestamp(2212L))
        );

        list.add(new CatalogSchemaDescriptor(
                state.id(),
                state.name("schema"),
                tables,
                indexes,
                systemViews,
                hybridTimestamp(435546L))
        );

        return list;
    }
}
