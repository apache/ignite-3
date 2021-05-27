/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.schema;

import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.schema.SortOrder;
import org.apache.ignite.schema.SortedIndexColumn;

/**
 * Ordered index column.
 */
public class SortedIndexColumnImpl extends AbstractSchemaObject implements SortedIndexColumn {
    /** Sort order. */
    private final SortOrder sortOrder;

    /**
     * Constructor.
     *
     * @param name Column name.
     * @param sortOrder Sort order flag.
     */
    public SortedIndexColumnImpl(String name, SortOrder sortOrder) {
        super(name);

        this.sortOrder = sortOrder;
    }

    /** {@inheritDoc} */
    @Override public SortOrder sortOrder() {
        return sortOrder;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(SortedIndexColumnImpl.class, this);
    }
}
