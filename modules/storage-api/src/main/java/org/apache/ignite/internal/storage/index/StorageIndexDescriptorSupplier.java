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

package org.apache.ignite.internal.storage.index;

import static org.apache.ignite.internal.catalog.descriptors.CatalogDescriptorUtils.toIndexDescriptor;
import static org.apache.ignite.internal.catalog.descriptors.CatalogDescriptorUtils.toTableDescriptor;
import static org.apache.ignite.internal.schema.configuration.SchemaConfigurationUtils.findIndexView;
import static org.apache.ignite.internal.schema.configuration.SchemaConfigurationUtils.findTableView;

import org.apache.ignite.internal.schema.configuration.TableView;
import org.apache.ignite.internal.schema.configuration.TablesConfiguration;
import org.apache.ignite.internal.schema.configuration.TablesView;
import org.apache.ignite.internal.schema.configuration.index.TableIndexView;
import org.jetbrains.annotations.Nullable;

/**
 * Index descriptor supplier.
 */
// TODO: IGNITE-19717 Get rid of it
public class StorageIndexDescriptorSupplier {
    private final TablesConfiguration tablesConfig;

    /**
     * Constructor.
     *
     * @param tablesConfig Tables configuration.
     */
    public StorageIndexDescriptorSupplier(TablesConfiguration tablesConfig) {
        this.tablesConfig = tablesConfig;
    }

    /**
     * Returns an index descriptor by its ID, {@code null} if absent.
     *
     * @param id Index ID.
     */
    @Nullable
    public StorageIndexDescriptor get(int id) {
        TablesView tablesView = tablesConfig.value();

        TableIndexView indexView = findIndexView(tablesView, id);

        if (indexView == null) {
            return null;
        }

        TableView tableView = findTableView(tablesView, indexView.tableId());

        assert tableView != null : "tableId=" + indexView.tableId() + " ,indexId=" + id;

        return StorageIndexDescriptor.create(toTableDescriptor(tableView), toIndexDescriptor(indexView));
    }
}
