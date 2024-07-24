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

package org.apache.ignite.internal.table.distributed.replicator;

import org.apache.ignite.internal.replicator.exception.ExpectedReplicationException;
import org.apache.ignite.internal.tx.TransactionInternalException;
import org.apache.ignite.lang.ErrorGroups.Transactions;

/**
 * Thrown when, during an attempt to execute a transactional operation, it turns out that the operation cannot be executed
 * because an incompatible schema change has happened.
 */
public class IncompatibleSchemaVersionException extends TransactionInternalException implements ExpectedReplicationException {
    private static final String SCHEMA_CHANGED_MESSAGE = "Table schema was updated after the transaction was started "
            + "[table=%s, startSchema=%d, operationSchema=%d]";

    private static final String TABLE_DROPPED_NAME_MESSAGE = "Table was dropped [table=%s]";

    private static final String TABLE_DROPPED_ID_MESSAGE = "Table was dropped [tableId=%d]";

    public IncompatibleSchemaVersionException(String message) {
        super(Transactions.TX_INCOMPATIBLE_SCHEMA_ERR, message);
    }

    /**
     * Returns new IncompatibleSchemaException for a case when schema was updated after the beginning of the transaction.
     *
     * @param tableName Name of the table.
     * @param startSchemaVersion Schema version at the beginning of the transaction.
     * @param operationSchemaVersion Schema version at the moment of the operation.
     * @return Exception with formatted message.
     */
    public static IncompatibleSchemaVersionException schemaChanged(String tableName, int startSchemaVersion, int operationSchemaVersion) {
        return new IncompatibleSchemaVersionException(String.format(
                SCHEMA_CHANGED_MESSAGE,
                tableName, startSchemaVersion, operationSchemaVersion
        ));
    }

    /**
     * Returns new IncompatibleSchemaException for a case when the table was dropped at the moment of operation.
     *
     * @param tableName Name of the table.
     * @return Exception with formatted message.
     */
    public static IncompatibleSchemaVersionException tableDropped(String tableName) {
        return new IncompatibleSchemaVersionException(String.format(TABLE_DROPPED_NAME_MESSAGE, tableName));
    }

    /**
     * Returns new IncompatibleSchemaException for a case when table was dropped at the moment of operation.
     *
     * @param tableId ID of the table.
     * @return Exception with formatted message.
     */
    // TODO https://issues.apache.org/jira/browse/IGNITE-22309 use tableName instead
    public static IncompatibleSchemaVersionException tableDropped(int tableId) {
        return new IncompatibleSchemaVersionException(String.format(TABLE_DROPPED_ID_MESSAGE, tableId));
    }
}
