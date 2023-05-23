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

package org.apache.ignite.internal.sql.engine.prepare.ddl;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.ignite.internal.catalog.commands.DefaultValue;
import org.apache.ignite.internal.catalog.commands.altercolumn.ChangeColumnType;
import org.apache.ignite.internal.catalog.commands.altercolumn.ChangeColumnDefault;
import org.apache.ignite.internal.catalog.commands.altercolumn.ChangeColumnNotNull;
import org.apache.ignite.internal.catalog.commands.altercolumn.ColumnChanger;
import org.apache.ignite.internal.sql.engine.util.TypeUtils;
import org.apache.ignite.sql.ColumnType;

/**
 * ALTER TABLE ... ALTER COLUMN statement.
 */
@SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType")
public class AlterColumnCommand extends AbstractTableDdlCommand {
    /** Column. */
    private String columnName;

    private List<ColumnChanger> actions = new ArrayList<>(1);

    public String columnName() {
        return columnName;
    }

    public void columnName(String name) {
        columnName = name;
    }

    public void addChange(RelDataType type) {
        actions.add(new ChangeColumnType(TypeUtils.columnType(type), type.getPrecision(), type.getScale()));
    }

    public void addChange(boolean notNull) {
        actions.add(new ChangeColumnNotNull(notNull));
    }

    public void addChange(Function<ColumnType, DefaultValue> resolveDfltFunc) {
        actions.add(new ChangeColumnDefault(resolveDfltFunc));
    }

    public List<ColumnChanger> actions() {
        return actions;
    }
}
