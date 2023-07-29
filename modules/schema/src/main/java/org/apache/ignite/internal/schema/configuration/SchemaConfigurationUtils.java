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

package org.apache.ignite.internal.schema.configuration;

import org.apache.ignite.configuration.NamedListView;
import org.jetbrains.annotations.Nullable;

/**
 * Helper class for working with schema configuration.
 */
public class SchemaConfigurationUtils {
    /**
     * Looks up the table configuration by ID, {@code null} if absent.
     *
     * @param tablesView Tables configuration.
     * @param tableId Table ID.
     */
    public static @Nullable TableView findTableView(TablesView tablesView, int tableId) {
        return findTableView(tablesView.tables(), tableId);
    }

    /**
     * Looks up the table configuration by ID, {@code null} if absent.
     *
     * @param tableListView Table list configuration.
     * @param tableId Table ID.
     */
    public static @Nullable TableView findTableView(NamedListView<? extends TableView> tableListView, int tableId) {
        return tableListView.stream()
                .filter(tableView -> tableId == tableView.id())
                .findFirst()
                .orElse(null);
    }
}
