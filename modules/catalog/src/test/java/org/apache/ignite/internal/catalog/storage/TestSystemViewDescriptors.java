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
import org.apache.ignite.internal.catalog.descriptors.CatalogSystemViewDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogSystemViewDescriptor.SystemViewType;

/**
 * Random {@link CatalogSystemViewDescriptor}S for testing.
 */
final class TestSystemViewDescriptors {

    /**
     * Generates a list of {@link CatalogSystemViewDescriptor}s of the given version.
     *
     * @param state Random state,
     * @param version Version.l
     * @return A list of descriptors.
     */
    static List<CatalogSystemViewDescriptor> systemViews(TestDescriptorState state, int version) {
        switch (version) {
            case 1:
            case 2:
                return systemViewsV0(state);
            default:
                throw new IllegalArgumentException("Unexpected CatalogSystemViewDescriptor version: " + version);
        }
    }

    private static List<CatalogSystemViewDescriptor> systemViewsV0(TestDescriptorState state) {
        List<CatalogSystemViewDescriptor> list = new ArrayList<>();

        list.add(new CatalogSystemViewDescriptor(
                state.id(),
                state.id(),
                state.name("SYS_VIEW"),
                TestTableColumnDescriptors.columns(state),
                SystemViewType.NODE, hybridTimestamp(90323L))
        );
        list.add(new CatalogSystemViewDescriptor(
                state.id(),
                state.id(),
                state.name("SYS_VIEW"),
                TestTableColumnDescriptors.columns(state),
                SystemViewType.CLUSTER,
                hybridTimestamp(213232L))
        );

        return list;
    }
}
