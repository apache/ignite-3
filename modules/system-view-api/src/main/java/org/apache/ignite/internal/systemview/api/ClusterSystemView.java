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

package org.apache.ignite.internal.systemview.api;

import java.util.List;
import java.util.concurrent.Flow.Publisher;
import org.apache.ignite.internal.tostring.S;

/**
 * Cluster wide view definition.
 *
 * <pre>
 *   // Creates definition of a cluster-wide system view.
 *   var tablesView = SystemViews.&lt;Table&gt;clusterViewBuilder()
 *     .name("TABLES")
 *     .addColumn("ID", Integer.class, Table::id)
 *     .addColumn("SCHEMA_ID", Integer.class, Table::schemaId)
 *     .addColumn("NAME", String.class, Table::name)
 *     .dataProvider(() -> toAsyncCursor(tableManager.tables()))
 *     .build();
 * </pre>
 *
 * @param <T> System view data type.
 * @see SystemView
 */
public class ClusterSystemView<T> extends SystemView<T> {

    /**
     * Constructor.
     *
     * @param name View name.
     * @param columns List of columns.
     * @param dataProvider Data provider.
     */
    private ClusterSystemView(String name,
            List<SystemViewColumn<T, ?>> columns,
            Publisher<T> dataProvider) {

        super(name, columns, dataProvider);
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return S.toString(ClusterSystemView.class, this, "name", name(), "columns", columns());
    }

    /**
     * Cluster-wide system view builder.
     *
     * @param <T> System view data type.
     */
    public static class Builder<T> extends SystemViewBuilder<ClusterSystemView<T>, T, Builder<T>> {

        /** Constructor. */
        Builder() {

        }

        /**
         * Creates an instance of {@link ClusterSystemView}.
         *
         * @return Definition of a cluster-wide system view.
         */
        @Override
        public ClusterSystemView<T> build() {
            return new ClusterSystemView<>(name, columns, dataProvider);
        }
    }
}
