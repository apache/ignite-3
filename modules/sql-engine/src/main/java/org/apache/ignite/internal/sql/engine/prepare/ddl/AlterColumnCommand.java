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
import org.jetbrains.annotations.Nullable;

/**
 * ALTER TABLE ... ALTER COLUMN statement.
 */
@SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType")
public class AlterColumnCommand extends AbstractTableDdlCommand {
    public interface Action {
        ColumnChanger toParams();
    }

    static class ChangeType implements Action {
        private final RelDataType type;

        public ChangeType(RelDataType type) {
            this.type = type;
        }

        public RelDataType type() {
            return type;
        }

        @Override
        public ColumnChanger toParams() {
            return new ChangeColumnType(TypeUtils.columnType(type), type.getPrecision(), type.getScale());
        }
    }

    static class ChangeDefault implements Action {
        private final Function<ColumnType, DefaultValue> resolveDfltFunc;

        public ChangeDefault(Function<ColumnType, DefaultValue> resolveDfltFunc) {
            this.resolveDfltFunc = resolveDfltFunc;
        }

        @Override
        public ColumnChanger toParams() {
            return new ChangeColumnDefault(resolveDfltFunc);
        }
    }

    static class ChangeNotNull implements Action {
        private final boolean notNull;

        public ChangeNotNull(boolean notNull) {
            this.notNull = notNull;
        }

        public boolean notNull() {
            return notNull;
        }

        @Override
        public ColumnChanger toParams() {
            return new ChangeColumnNotNull(notNull);
        }
    }

    /** Column. */
    private String columnName;

    private List<Action> actions = new ArrayList<>(1);

    public String columnName() {
        return columnName;
    }

    public void columnName(String name) {
        columnName = name;
    }

    public void addAction(Action action) {
        actions.add(action);
    }

    public List<Action> actions() {
        return actions;
    }
}
