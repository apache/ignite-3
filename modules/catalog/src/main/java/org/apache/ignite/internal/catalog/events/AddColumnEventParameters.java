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
import org.apache.ignite.internal.catalog.descriptors.CatalogTableColumnDescriptor;

/**
 * Add column event parameters contains descriptors of added columns.
 */
public class AddColumnEventParameters extends TableEventParameters {

    private final List<CatalogTableColumnDescriptor> columnDescriptors;

    /**
     * Constructor.
     *
     * @param causalityToken Causality token.
     * @param catalogVersion Catalog version.
     * @param tableId An id of table, which columns are added to.
     * @param columnDescriptors New columns descriptors.
     */
    public AddColumnEventParameters(
            long causalityToken,
            int catalogVersion,
            int tableId,
            List<CatalogTableColumnDescriptor> columnDescriptors
    ) {
        super(causalityToken, catalogVersion, tableId);

        this.columnDescriptors = columnDescriptors;
    }

    /**
     * Returns descriptors of columns to add.
     */
    public List<CatalogTableColumnDescriptor> descriptors() {
        return columnDescriptors;
    }
}
