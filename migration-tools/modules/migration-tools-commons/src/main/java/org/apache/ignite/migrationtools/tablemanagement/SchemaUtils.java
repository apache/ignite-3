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
