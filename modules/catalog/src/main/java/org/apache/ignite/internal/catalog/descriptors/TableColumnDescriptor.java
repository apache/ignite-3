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

import java.io.Serializable;
import java.util.Objects;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.sql.ColumnType;

/**
 * Table column descriptor.
 */
public class TableColumnDescriptor implements Serializable {
    private static final long serialVersionUID = 7684890562398520509L;

    private final String name;
    private final ColumnType type;
    private final boolean nullable;
    /** Max length constraint. */
    private int length;
    private int precision;
    private int scale;
    private String defaultValueExpression;

    public TableColumnDescriptor(String name, ColumnType type, boolean nullable) {
        this.name = Objects.requireNonNull(name, "name");
        this.type = Objects.requireNonNull(type);
        this.nullable = nullable;
    }

    public String name() {
        return name;
    }

    public boolean nullable() {
        return nullable;
    }

    public ColumnType type() {
        return type;
    }

    public int precision() {
        return precision;
    }

    public int scale() {
        return scale;
    }

    public int length() {
        return length;
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return S.toString(this);
    }
}
