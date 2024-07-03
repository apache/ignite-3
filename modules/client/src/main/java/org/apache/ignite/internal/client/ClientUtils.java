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

package org.apache.ignite.internal.client;

import org.apache.ignite.client.ClientOperationType;
import org.apache.ignite.client.IgniteClientConfiguration;
import org.apache.ignite.internal.client.proto.ClientOp;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.lang.LoggerFactory;

/**
 * Client utilities.
 */
public class ClientUtils {
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

            case ClientOp.TUPLE_CONTAINS_ALL_KEYS:
                return ClientOperationType.TUPLE_CONTAINS_ALL_KEYS;

            case ClientOp.JDBC_CONNECT:
                return null;

            case ClientOp.JDBC_EXEC:
                return null;

            case ClientOp.JDBC_NEXT:
                return null;

            case ClientOp.JDBC_MORE_RESULTS:
                return null;

            case ClientOp.JDBC_EXEC_BATCH:
                return null;

            case ClientOp.JDBC_CURSOR_CLOSE:
                return null;

            case ClientOp.JDBC_TABLE_META:
                return null;

            case ClientOp.JDBC_COLUMN_META:
                return null;

            case ClientOp.JDBC_SCHEMAS_META:
                return null;

            case ClientOp.JDBC_PK_META:
                return null;

            case ClientOp.TX_BEGIN:
            case ClientOp.TX_COMMIT:
            case ClientOp.TX_ROLLBACK:
                return null; // Commit/rollback use owning connection and bypass retry mechanism.

            case ClientOp.JDBC_SQL_EXEC_PS_BATCH:
                return null;

            case ClientOp.COMPUTE_EXECUTE:
            case ClientOp.COMPUTE_EXECUTE_COLOCATED:
                return ClientOperationType.COMPUTE_EXECUTE;

            case ClientOp.COMPUTE_EXECUTE_MAPREDUCE:
                return ClientOperationType.COMPUTE_EXECUTE_MAPREDUCE;

            case ClientOp.COMPUTE_GET_STATE:
                return ClientOperationType.COMPUTE_GET_STATE;

            case ClientOp.COMPUTE_CANCEL:
                return ClientOperationType.COMPUTE_CANCEL;

            case ClientOp.COMPUTE_CHANGE_PRIORITY:
                return ClientOperationType.COMPUTE_CHANGE_PRIORITY;

            case ClientOp.CLUSTER_GET_NODES:
                return null;

            case ClientOp.SQL_EXEC:
                return ClientOperationType.SQL_EXECUTE;

            case ClientOp.SQL_EXEC_SCRIPT:
                return ClientOperationType.SQL_EXECUTE_SCRIPT;

            case ClientOp.SQL_CURSOR_NEXT_PAGE:
                return ClientOperationType.SQL_CURSOR_NEXT_PAGE;

            case ClientOp.SQL_CURSOR_CLOSE:
                return null;

            case ClientOp.PARTITION_ASSIGNMENT_GET:
                return null;

            case ClientOp.JDBC_TX_FINISH:
                return null;

            case ClientOp.SQL_QUERY_META:
                return null;

            case ClientOp.STREAMER_BATCH_SEND:
                return ClientOperationType.STREAMER_BATCH_SEND;

            case ClientOp.STREAMER_WITH_RECEIVER_BATCH_SEND:
                return ClientOperationType.STREAMER_WITH_RECEIVER_BATCH_SEND;

            case ClientOp.SQL_EXEC_BATCH:
                return ClientOperationType.SQL_EXECUTE_BATCH;

            case ClientOp.PRIMARY_REPLICAS_GET:
                return ClientOperationType.PRIMARY_REPLICAS_GET;

            // Do not return null from default arm intentionally, so we don't forget to update this when new ClientOp values are added.
            default:
                throw new UnsupportedOperationException("Invalid op code: " + opCode);
        }
    }

    /**
     * Gets a logger for the given class.
     *
     * @param cls Class.
     * @return Logger.
     */
    public static <T> IgniteLogger logger(IgniteClientConfiguration cfg, Class<T> cls) {
        var loggerFactory = cfg.loggerFactory() == null
                ? (LoggerFactory) System::getLogger
                : cfg.loggerFactory();

        return loggerFactory == null
                ? Loggers.voidLogger()
                : Loggers.forClass(cls, loggerFactory);
    }
}
