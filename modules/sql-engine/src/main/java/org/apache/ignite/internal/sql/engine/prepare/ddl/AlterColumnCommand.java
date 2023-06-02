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

import java.util.function.Function;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.ignite.internal.catalog.commands.DefaultValue;
import org.apache.ignite.sql.ColumnType;
import org.jetbrains.annotations.Nullable;

/**
 * ALTER TABLE ... ALTER COLUMN statement.
 */
public class AlterColumnCommand extends AbstractTableDdlCommand {
    private String columnName;

    private RelDataType type;

    private Boolean notNull;

    private Function<ColumnType, DefaultValue> resolveDfltFunc;

    public String columnName() {
        return columnName;
    }

    void columnName(String name) {
        columnName = name;
    }

    void type(RelDataType type) {
        this.type = type;
    }

    @Nullable public RelDataType type() {
        return type;
    }

    void notNull(boolean notNull) {
        this.notNull = notNull;
    }

    @Nullable public Boolean notNull() {
        return notNull;
    }

    void defaultResolver(Function<ColumnType, DefaultValue> resolveDfltFunc) {
        this.resolveDfltFunc = resolveDfltFunc;
    }

    @Nullable public Function<ColumnType, DefaultValue> defaultResolver() {
        return resolveDfltFunc;
    }
}
