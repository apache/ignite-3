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

import java.util.function.Function;
import org.apache.ignite.internal.catalog.commands.DefaultValue;
import org.apache.ignite.internal.catalog.descriptors.TableColumnDescriptor;
import org.apache.ignite.sql.ColumnType;

/**
 * Replaces default value of the column descriptor according to the {@code ALTER COLUMN (SET | DROP) DEFAULT} action.
 */
public class AlterColumnDefault implements AlterColumnAction {
    private final Function<ColumnType, DefaultValue> resolveDfltFunc;

    public AlterColumnDefault(Function<ColumnType, DefaultValue> resolveDfltFunc) {
        this.resolveDfltFunc = resolveDfltFunc;
    }

    @Override
    public TableColumnDescriptor apply(TableColumnDescriptor origin, boolean isPkColumn) {
        DefaultValue dflt = resolveDfltFunc.apply(origin.type());

        if (dflt.equals(origin.defaultValue())) {
            return origin;
        }

        return new TableColumnDescriptor(
                origin.name(), origin.type(), origin.nullable(), dflt, origin.precision(), origin.scale(), origin.length());
    }
}
