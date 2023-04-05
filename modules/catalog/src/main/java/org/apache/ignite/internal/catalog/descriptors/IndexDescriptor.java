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

package org.apache.ignite.internal.catalog.descriptors;

import org.apache.ignite.internal.tostring.S;

/**
 * Index descriptor base class.
 */
public abstract class IndexDescriptor extends ObjectDescriptor {
    private static final long serialVersionUID = -8045949593661301287L;

    /** Table id. */
    private final int tableId;

    /** Unique constraint flag. */
    private boolean unique;

    /** Write only flag. {@code True} when index is building. */
    private boolean writeOnly;

    IndexDescriptor(int id, String name, int tableId, boolean unique) {
        super(id, Type.INDEX, name);
        this.tableId = tableId;
        this.unique = unique;
    }

    public int tableId() {
        return tableId;
    }

    public boolean unique() {
        return unique;
    }

    public boolean writeOnly() {
        return writeOnly;
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return S.toString(this);
    }
}
