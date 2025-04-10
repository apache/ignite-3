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

/** This interface provides a registry for mappings between tables and their corresponding java types. */
public interface TableTypeRegistry {
    /**
     * Gets the available type hints for the table.
     *
     * @param tableName Must be escaped according to Ignite 3 rules.
     * @return The type hints for the table, or null if none are available.
     */
    @Nullable Map.Entry<Class<?>, Class<?>> typesForTable(String tableName) throws ClassNotFoundException;

    /**
     * Registers the supplied type hints for the given table. Existing hints will be replaced.
     *
     * @param tableName Must be escaped according to Ignite 3 rules.
     * @param tableTypes Entry with the ClassNames of Key and Value types, as returned by {@link Class#getName()}.
     */
    void registerTypesForTable(String tableName, Map.Entry<String, String> tableTypes);

    static Map.Entry<String, String> typeHints(Class<?> keyType, Class<?> valType) {
        return Map.entry(keyType.getName(), valType.getName());
    }
}
