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

import org.apache.ignite.schema.Column;
import org.apache.ignite.schema.ColumnType;

public class ColumnImpl implements Column {

    private final String name;

    public ColumnImpl(String name) {
        this.name = name;
    }

    /** {@inheritDoc} */
    @Override public ColumnType type() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return name;
    }

    /** {@inheritDoc} */
    @Override public boolean nullable() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public Object defaultValue() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean isKeyColumn() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean isAffinityColumn() {
        return false;
    }
}
