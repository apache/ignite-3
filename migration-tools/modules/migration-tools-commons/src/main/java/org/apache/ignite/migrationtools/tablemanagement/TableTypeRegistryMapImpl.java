/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
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
