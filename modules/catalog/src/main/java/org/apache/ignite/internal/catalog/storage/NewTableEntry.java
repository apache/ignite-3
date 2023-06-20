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

import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.tostring.S;

/**
 * Describes addition of a new table.
 */
public class NewTableEntry implements UpdateEntry {
    private static final long serialVersionUID = 2970125889493580121L;

    private final CatalogTableDescriptor descriptor;

    /**
     * Constructs the object.
     *
     * @param descriptor A descriptor of a table to add.
     */
    public NewTableEntry(CatalogTableDescriptor descriptor) {
        this.descriptor = descriptor;
    }

    /** Returns descriptor of a table to add. */
    public CatalogTableDescriptor descriptor() {
        return descriptor;
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return S.toString(this);
    }
}
