/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.migrationtools.tablemanagement;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite3.internal.client.table.ClientSchema;
import org.apache.ignite3.internal.client.table.ClientTable;

/** Utility methods to interact with internal client schemas. */
public class SchemaUtils {
    private static final Method GET_LATEST_SCHEMA_METHOD;

    static {
        try {
            GET_LATEST_SCHEMA_METHOD = ClientTable.class.getDeclaredMethod("getLatestSchema");
            GET_LATEST_SCHEMA_METHOD.setAccessible(true);
        } catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }

    private SchemaUtils() {
        // Intentionally left blank
    }

    /** Exposes {@link ClientTable#getLatestSchema()}. */
    public static CompletableFuture<ClientSchema> getLatestSchemaForTable(ClientTable clientTable) {
        try {
            return (CompletableFuture<ClientSchema>) GET_LATEST_SCHEMA_METHOD.invoke(clientTable);
        } catch (IllegalAccessException | InvocationTargetException e) {
            return CompletableFuture.failedFuture(e);
        }
    }
}
