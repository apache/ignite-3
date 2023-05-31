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

/**
 * Changes {@code nullable} flag of the column descriptor according to the {@code ALTER COLUMN (SET | DROP) NOT NULL} action.
 */
public class AlterColumnNotNull implements AlterColumnAction {
    private final boolean notNull;

    public AlterColumnNotNull(boolean notNull) {
        this.notNull = notNull;
    }

    @Override
    public TableColumnDescriptor apply(TableColumnDescriptor origin, boolean isPkColumn) {
        if (notNull == !origin.nullable()) {
            return origin;
        }

        // Set NOT NULL constraint is not supported.
        if (notNull) {
            throw new SqlException(UNSUPPORTED_DDL_OPERATION_ERR,
                    IgniteStringFormatter.format("Cannot set NOT NULL for column '{}'.", origin.name()));
        }

        if (isPkColumn) {
            throw new SqlException(UNSUPPORTED_DDL_OPERATION_ERR,
                    IgniteStringFormatter.format("Cannot drop NOT NULL for the primary key column '{}'.", origin.name()));
        }

        return new TableColumnDescriptor(
                origin.name(), origin.type(), true, origin.defaultValue(), origin.precision(), origin.scale(), origin.length());
    }
}
