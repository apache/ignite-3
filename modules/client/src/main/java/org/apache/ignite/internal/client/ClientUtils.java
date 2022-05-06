/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.client;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.apache.ignite.client.ClientOperationType;
import org.apache.ignite.client.IgniteClientException;
import org.apache.ignite.internal.client.proto.ClientOp;
import org.apache.ignite.lang.IgniteException;

/**
 * Client utilities.
 */
public class ClientUtils {
    /**
     * Waits for async operation completion.
     *
     * @param fut Future to wait to.
     * @param <T> Future result type.
     * @return Future result.
     */
    public static <T> T sync(CompletableFuture<T> fut) {
        try {
            return fut.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt(); // Restore interrupt flag.

            throw convertException(e);
        } catch (ExecutionException e) {
            throw convertException(e.getCause());
        }
    }

    /**
     * Converts an internal exception to a public one.
     *
     * @param e Internal exception.
     * @return Public exception.
     */
    public static IgniteException convertException(Throwable e) {
        if (e instanceof IgniteException) {
            return (IgniteException) e;
        }

        //TODO: IGNITE-14500 Replace with public exception with an error code (or unwrap?).
        return new IgniteClientException(e.getMessage(), e);
    }

    /**
     * Converts internal op code to public {@link ClientOperationType}.
     *
     * @param opCode Op code.
     * @return Operation type, or null for system operations.
     */
    public static ClientOperationType opCodeToClientOperationType(int opCode) {
        if (opCode < 0) {
            // No-op.
            return null;
        }

        switch (opCode) {
            case ClientOp.HEARTBEAT:
                return null;

            case ClientOp.TABLES_GET:
                return ClientOperationType.TABLES_GET;

            case ClientOp.TABLE_GET:
                return ClientOperationType.TABLE_GET;

            case ClientOp.SCHEMAS_GET:
                return null;

            case ClientOp.TUPLE_UPSERT:
                return ClientOperationType.TUPLE_UPSERT;

            case ClientOp.TUPLE_GET:
                return ClientOperationType.TUPLE_GET;

            case ClientOp.TUPLE_UPSERT_ALL:
                return ClientOperationType.TUPLE_UPSERT_ALL;

            case ClientOp.TUPLE_GET_ALL:
                return ClientOperationType.TUPLE_GET_ALL;

            case ClientOp.TUPLE_GET_AND_UPSERT:
                return ClientOperationType.TUPLE_GET_AND_UPSERT;

            case ClientOp.TUPLE_INSERT:
                return ClientOperationType.TUPLE_INSERT;

            case ClientOp.TUPLE_INSERT_ALL:
                return ClientOperationType.TUPLE_INSERT_ALL;

            case ClientOp.TUPLE_REPLACE:
                return ClientOperationType.TUPLE_REPLACE;

            case ClientOp.TUPLE_REPLACE_EXACT:
                return ClientOperationType.TUPLE_REPLACE_EXACT;

            case ClientOp.TUPLE_GET_AND_REPLACE:
                return ClientOperationType.TUPLE_GET_AND_REPLACE;

            case ClientOp.TUPLE_DELETE:
                return ClientOperationType.TUPLE_DELETE;

            case ClientOp.TUPLE_DELETE_ALL:
                return ClientOperationType.TUPLE_DELETE_ALL;

            case ClientOp.TUPLE_DELETE_EXACT:
                return ClientOperationType.TUPLE_DELETE_EXACT;

            case ClientOp.TUPLE_DELETE_ALL_EXACT:
                return ClientOperationType.TUPLE_DELETE_ALL_EXACT;

            case ClientOp.TUPLE_GET_AND_DELETE:
                return ClientOperationType.TUPLE_GET_AND_DELETE;

            case ClientOp.TUPLE_CONTAINS_KEY:
                return ClientOperationType.TUPLE_CONTAINS_KEY;

            case ClientOp.SQL_EXEC:
                return null;

            case ClientOp.SQL_NEXT:
                return null;

            case ClientOp.SQL_EXEC_BATCH:
                return null;

            case ClientOp.SQL_CURSOR_CLOSE:
                return null;

            case ClientOp.SQL_TABLE_META:
                return null;

            case ClientOp.SQL_COLUMN_META:
                return null;

            case ClientOp.SQL_SCHEMAS_META:
                return null;

            case ClientOp.SQL_PK_META:
                return null;

            case ClientOp.SQL_QUERY_META:
                return null;

            case ClientOp.TX_BEGIN:
            case ClientOp.TX_COMMIT:
            case ClientOp.TX_ROLLBACK:
                return null; // Commit/rollback use owning connection and bypass retry mechanism.

            case ClientOp.SQL_EXEC_PS_BATCH:
                return null;

            case ClientOp.COMPUTE_EXECUTE:
            case ClientOp.COMPUTE_EXECUTE_COLOCATED:
                return ClientOperationType.COMPUTE_EXECUTE;

            case ClientOp.CLUSTER_GET_NODES:
                return null;

            // Do not return null from default arm intentionally, so we don't forget to update this when new ClientOp values are added.
            default:
                throw new UnsupportedOperationException("Invalid op code: " + opCode);
        }
    }
}
