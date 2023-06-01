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

package org.apache.ignite.internal.catalog.events;

import java.util.List;
import org.apache.ignite.internal.catalog.descriptors.TableColumnDescriptor;

/**
 * Add column event parameters contains descriptors of added columns.
 */
public class AddColumnEventParameters extends CatalogEventParameters {

    private final int tableId;
    private final List<TableColumnDescriptor> columnDescriptors;

    /**
     * Constructor.
     *
     * @param causalityToken Causality token.
     * @param tableId An id of table, which columns are added to.
     * @param columnDescriptors New columns descriptors.
     */
    public AddColumnEventParameters(long causalityToken, int tableId, List<TableColumnDescriptor> columnDescriptors) {
        super(causalityToken);

        this.tableId = tableId;
        this.columnDescriptors = columnDescriptors;
    }

    /** Returns table id. */
    public int tableId() {
        return tableId;
    }

    /**
     * Returns descriptors of columns to add.
     */
    public List<TableColumnDescriptor> descriptors() {
        return columnDescriptors;
    }
}
