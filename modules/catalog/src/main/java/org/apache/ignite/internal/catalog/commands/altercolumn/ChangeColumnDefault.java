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

import java.util.function.Function;
import org.apache.ignite.internal.catalog.commands.DefaultValue;
import org.apache.ignite.internal.catalog.commands.DefaultValue.Type;
import org.apache.ignite.internal.catalog.descriptors.TableColumnDescriptor;
import org.apache.ignite.sql.ColumnType;
import org.apache.ignite.sql.SqlException;
import org.jetbrains.annotations.Nullable;

/**
 * Changes {@code default} expression of the column descriptor according to the {@code ALTER COLUMN (SET | DROP) DEFAULT} action.
 */
public class ChangeColumnDefault implements ColumnChangeAction {
    private final Function<ColumnType, DefaultValue> resolveDfltFunc;

    public ChangeColumnDefault(Function<ColumnType, DefaultValue> resolveDfltFunc) {
        this.resolveDfltFunc = resolveDfltFunc;
    }

    @Override
    public @Nullable TableColumnDescriptor apply(TableColumnDescriptor origin) {
        DefaultValue dflt = resolveDfltFunc.apply(origin.type());

        if (dflt.equals(origin.defaultValue())) {
            return null;
        }

        if (dflt.type() == Type.CONSTANT && ((DefaultValue.ConstantValue) dflt).value() == null) {
            throw new SqlException(UNSUPPORTED_DDL_OPERATION_ERR, "Cannot drop default for column " + origin.name());
        }

        return new TableColumnDescriptor(origin.name(), origin.type(), origin.nullable(), dflt, origin.precision(), origin.scale(),
                origin.length());
    }

    @Override
    public Priority priority() {
        return Priority.DEFAULT;
    }
}
