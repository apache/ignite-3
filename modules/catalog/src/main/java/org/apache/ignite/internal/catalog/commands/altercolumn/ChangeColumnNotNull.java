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

package org.apache.ignite.internal.catalog.commands.altercolumn;

import static org.apache.ignite.lang.ErrorGroups.Sql.UNSUPPORTED_DDL_OPERATION_ERR;

import org.apache.ignite.internal.catalog.descriptors.TableColumnDescriptor;
import org.apache.ignite.lang.IgniteStringFormatter;
import org.apache.ignite.sql.SqlException;
import org.jetbrains.annotations.Nullable;

public class ChangeColumnNotNull implements ColumnChanger {
    private final boolean notNull;

    public ChangeColumnNotNull(boolean notNull) {
        this.notNull = notNull;
    }

    @Override
    public @Nullable TableColumnDescriptor apply(TableColumnDescriptor source) {
        if (notNull == !source.nullable()) {
            return null;
        } else if (!notNull) {
            return new TableColumnDescriptor(source.name(), source.type(), true, source.defaultValue())
                    .scale(source.scale())
                    .precision(source.precision())
                    .length(source.length());
        }

        throw new SqlException(UNSUPPORTED_DDL_OPERATION_ERR,
                IgniteStringFormatter.format("Cannot set NOT NULL for column '{}'.", source.name()));
    }

    @Override
    public Priority priority() {
        return notNull ? Priority.SET_NOT_NULL : Priority.DROP_NOT_NULL;
    }
}
