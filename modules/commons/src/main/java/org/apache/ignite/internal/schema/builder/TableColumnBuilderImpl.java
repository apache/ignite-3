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

package org.apache.ignite.internal.schema.builder;

import org.apache.ignite.internal.schema.ColumnImpl;
import org.apache.ignite.schema.Column;
import org.apache.ignite.schema.ColumnType;
import org.apache.ignite.schema.builder.TableColumnBuilder;

public class TableColumnBuilderImpl implements TableColumnBuilder {
    private String colName;
    private ColumnType colType;
    private boolean nullable;
    private Object defValue;

    public TableColumnBuilderImpl(String colName, ColumnType colType) {
        this.colName = colName;
        this.colType = colType;
    }

    /** {@inheritDoc} */
    @Override public TableColumnBuilderImpl asNullable() {
        nullable = true;

        return this;
    }

    /** {@inheritDoc} */
    @Override public TableColumnBuilderImpl asNonNull() {
        nullable = false;

        return this;
    }

    /** {@inheritDoc} */
    @Override public TableColumnBuilderImpl withDefaultValue(Object defValue) {
        this.defValue = defValue;

        return this;
    }

    String name() {
        return colName;
    }

    boolean isNullable() {
        return nullable;
    }

    /** {@inheritDoc} */
    @Override public Column build() {
        return new ColumnImpl(colName);
    }
}
