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

package org.apache.ignite.migrationtools.tablemanagement;

import java.util.Map;
import org.jetbrains.annotations.Nullable;

/** Decorator for {@link TableTypeRegistry} that only registers new hints. */
public class RegisterOnlyTableTypeRegistry implements TableTypeRegistry {
    private final TableTypeRegistry base;

    public RegisterOnlyTableTypeRegistry(TableTypeRegistry base) {
        this.base = base;
    }

    @Override
    @Nullable
    public Map.Entry<Class<?>, Class<?>> typesForTable(String tableName) throws ClassNotFoundException {
        return null;
    }

    @Override
    public void registerTypesForTable(String tableName, Map.Entry<String, String> tableTypes) {
        base.registerTypesForTable(tableName, tableTypes);
    }
}
