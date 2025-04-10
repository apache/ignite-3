/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
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
