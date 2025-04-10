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

import java.util.HashMap;
import java.util.Map;
import org.jetbrains.annotations.Nullable;

/** {@link TableTypeRegistry} implementation based on a in-memory map. */
public class TableTypeRegistryMapImpl implements TableTypeRegistry {
    private final Map<String, Map.Entry<String, String>> hints = new HashMap<>();

    @Override
    public @Nullable Map.Entry<Class<?>, Class<?>> typesForTable(String tableName) {
        var typeNames = hints.get(tableName);
        if (typeNames == null) {
            return null;
        }

        try {
            return Map.entry(Class.forName(typeNames.getKey()), Class.forName(typeNames.getValue()));
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void registerTypesForTable(String tableName, Map.Entry<String, String> tableTypes) {
        this.hints.putIfAbsent(tableName, tableTypes);
    }
}
