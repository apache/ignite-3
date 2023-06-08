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

import java.util.List;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableColumnDescriptor;
import org.apache.ignite.internal.tostring.S;

/**
 * Describes addition of new columns.
 */
public class NewColumnsEntry implements UpdateEntry {
    private static final long serialVersionUID = 2970125889493580121L;

    private final int tableId;
    private final List<CatalogTableColumnDescriptor> descriptors;

    /**
     * Constructs the object.
     *
     * @param tableId Table id.
     * @param descriptors Descriptors of columns to add.
     */
    public NewColumnsEntry(int tableId, List<CatalogTableColumnDescriptor> descriptors) {
        this.tableId = tableId;
        this.descriptors = descriptors;
    }

    /** Returns table id. */
    public int tableId() {
        return tableId;
    }

    /** Returns descriptors of columns to add. */
    public List<CatalogTableColumnDescriptor> descriptors() {
        return descriptors;
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return S.toString(this);
    }
}
