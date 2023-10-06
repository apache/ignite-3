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

package org.apache.ignite.lang;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import java.util.Locale;

/**
 * Defines error groups and its errors.
 */
@SuppressWarnings("PublicInnerClass")
public class ErrorGroups {
    /** List of all registered error groups. */
    private static final Int2ObjectMap<ErrorGroup> registeredGroups = new Int2ObjectOpenHashMap<>();

    /**
     * Initializes and register all error groups and error codes.
     */
    public static synchronized void initialize() {
        for (Class<?> cls : ErrorGroups.class.getDeclaredClasses()) {
            try {
                cls.getDeclaredConstructor().newInstance();
            } catch (Exception e) {
                throw new RuntimeException("Failed to initialize error groups", e);
            }
        }
    }

    /**
     * Creates a new error group with the given {@code groupName} and {@code groupCode}.
     *
     * @param groupName Group name to be created.
     * @param groupCode Group code to be created.
     * @return New error group.
     * @throws IllegalArgumentException If the specified name or group code already registered.
     *      Also, this exception is thrown if the given {@code groupName} is {@code null} or empty.
     */
    public static synchronized ErrorGroup registerGroup(String groupName, short groupCode) {
        if (groupName == null || groupName.isEmpty()) {
            throw new IllegalArgumentException("Group name is null or empty");
        }

        String grpName = groupName.toUpperCase(Locale.ENGLISH);

        if (registeredGroups.containsKey(groupCode)) {
            throw new IllegalArgumentException(
                    "Error group already registered [groupName=" + groupName + ", groupCode=" + groupCode
                            + ", registeredGroup=" + registeredGroups.get(groupCode) + ']');
        }

        for (ErrorGroup group : registeredGroups.values()) {
            if (group.name().equals(groupName)) {
                throw new IllegalArgumentException(
                        "Error group already registered [groupName=" + groupName + ", groupCode=" + groupCode
                                + ", registeredGroup=" + group + ']');
            }
        }

        ErrorGroup newGroup = new ErrorGroup(grpName, groupCode);

        registeredGroups.put(groupCode, newGroup);

        return newGroup;
    }

    /**
     * Returns group code extracted from the given full error code.
     *
     * @param code Full error code.
     * @return Group code.
     */
    public static short extractGroupCode(int code) {
        return (short) (code >>> 16);
    }

    /**
     * Returns error group identified by the given {@code groupCode}.
     *
     * @param groupCode Group code
     * @return Error Group.
     */
    public static ErrorGroup errorGroupByGroupCode(short groupCode) {
        return registeredGroups.get(groupCode);
    }

    /**
     * Returns error group identified by the given error {@code code}.
     *
     * @param code Full error code
     * @return Error Group.
     */
    public static ErrorGroup errorGroupByCode(int code) {
        return registeredGroups.get(extractGroupCode(code));
    }

    /** Common error group. */
    public static class Common {
        /** Common error group. */
        public static final ErrorGroup COMMON_ERR_GROUP = registerGroup("CMN", (short) 1);

        /** Node stopping error. */
        public static final int NODE_STOPPING_ERR = COMMON_ERR_GROUP.registerErrorCode((short) 1);

        /** Component not started error. */
        public static final int COMPONENT_NOT_STARTED_ERR = COMMON_ERR_GROUP.registerErrorCode((short) 2);

        /** Illegal argument or argument in a wrong format has been passed. */
        public static final int ILLEGAL_ARGUMENT_ERR = COMMON_ERR_GROUP.registerErrorCode((short) 3);

        /** SSL can not be configured error. */
        public static final int SSL_CONFIGURATION_ERR = COMMON_ERR_GROUP.registerErrorCode((short) 4);

        /** Operation failed because a node has left the cluster. */
        public static final int NODE_LEFT_ERR = COMMON_ERR_GROUP.registerErrorCode((short) 5);

        /**
         * This error code represents an internal error caused by faulty logic or coding in the Ignite codebase.
         * In general, this error code should be considered as a non-recoverable error
         */
        public static final int INTERNAL_ERR = COMMON_ERR_GROUP.registerErrorCode((short) 0xFFFF);
    }

    /** Tables error group. */
    public static class Table {
        /** Table error group. */
        public static final ErrorGroup TABLE_ERR_GROUP = registerGroup("TBL", (short) 2);

        /** Table already exists. */
        public static final int TABLE_ALREADY_EXISTS_ERR = TABLE_ERR_GROUP.registerErrorCode((short) 1);

        /** Table not found. */
        public static final int TABLE_NOT_FOUND_ERR = TABLE_ERR_GROUP.registerErrorCode((short) 2);

        /** Column already exists. */
        public static final int COLUMN_ALREADY_EXISTS_ERR = TABLE_ERR_GROUP.registerErrorCode((short) 3);

        /** Column not found. */
        public static final int COLUMN_NOT_FOUND_ERR = TABLE_ERR_GROUP.registerErrorCode((short) 4);

        /** Table is stopping. */
        public static final int TABLE_STOPPING_ERR = TABLE_ERR_GROUP.registerErrorCode((short) (5));

        /** Table definition is incorrect. */
        public static final int TABLE_DEFINITION_ERR = TABLE_ERR_GROUP.registerErrorCode((short) 6);

        /** Schema version mismatch. */
        public static final int SCHEMA_VERSION_MISMATCH_ERR = TABLE_ERR_GROUP.registerErrorCode((short) 7);
    }

    /** Client error group. */
    public static class Client {
        /** Client error group. */
        public static final ErrorGroup CLIENT_ERR_GROUP = registerGroup("CLIENT", (short) 3);

        /** Connection failed. */
        public static final int CONNECTION_ERR = CLIENT_ERR_GROUP.registerErrorCode((short) 1);

        /** Protocol breakdown. */
        public static final int PROTOCOL_ERR = CLIENT_ERR_GROUP.registerErrorCode((short) 2);

        /** Incompatible protocol version. */
        public static final int PROTOCOL_COMPATIBILITY_ERR = CLIENT_ERR_GROUP.registerErrorCode((short) 3);

        /** Table not found by ID. */
        public static final int TABLE_ID_NOT_FOUND_ERR = CLIENT_ERR_GROUP.registerErrorCode((short) 4);

        /** Authentication error. */
        public static final int AUTHENTICATION_ERR = CLIENT_ERR_GROUP.registerErrorCode((short) 5);

        /** Authorization error. */
        public static final int AUTHORIZATION_ERR = CLIENT_ERR_GROUP.registerErrorCode((short) 6);

        /** Configuration error. */
        public static final int CONFIGURATION_ERR = CLIENT_ERR_GROUP.registerErrorCode((short) 7);

        /** Cluster ID mismatch error. */
        public static final int CLUSTER_ID_MISMATCH_ERR = CLIENT_ERR_GROUP.registerErrorCode((short) 8);

        /** Client SSL configuration error. */
        public static final int CLIENT_SSL_CONFIGURATION_ERR = CLIENT_ERR_GROUP.registerErrorCode((short) 9);

        /** Client handshake header error. */
        public static final int HANDSHAKE_HEADER_ERR = CLIENT_ERR_GROUP.registerErrorCode((short) 10);
    }

    /** SQL error group. */
    public static class Sql {
        /** SQL error group. */
        public static final ErrorGroup SQL_ERR_GROUP = registerGroup("SQL", (short) 4);

        /** No more pages in the cursor error. */
        public static final int CURSOR_NO_MORE_PAGES_ERR = SQL_ERR_GROUP.registerErrorCode((short) 1);

        /** Query without a result set error. */
        public static final int QUERY_NO_RESULT_SET_ERR = SQL_ERR_GROUP.registerErrorCode((short) 2);

        /** Schema not found. */
        public static final int SCHEMA_NOT_FOUND_ERR = SQL_ERR_GROUP.registerErrorCode((short) 3);

        /** Cursor is already closed error. */
        public static final int CURSOR_CLOSED_ERR = SQL_ERR_GROUP.registerErrorCode((short) 4);

        /** Statement parsing error. This error is returned when an SQL statement string is not valid according to syntax rules. */
        public static final int STMT_PARSE_ERR = SQL_ERR_GROUP.registerErrorCode((short) 5);

        /**
         * Statement validation error. Although statement is grammatically correct, the semantic is in question.
         * This error may appear in following cases:
         * <ul>
         *     <li>the statement refer to relation that doesn't exists.</li>
         *     <li>the statement describes action that is prohibited by the system, like changing columns belonging to primary keys.</li>
         *     <li>the statement contains operation that is not defined for given operands' types, like addition of DATE and DECIMAL.</li>
         *     <li>etc</li>
         * </ul>
         *
         * <p>See message for details.
         */
        public static final int STMT_VALIDATION_ERR = SQL_ERR_GROUP.registerErrorCode((short) 6);

        /** Constraint violation error such as primary key violation. */
        public static final int CONSTRAINT_VIOLATION_ERR = SQL_ERR_GROUP.registerErrorCode((short) 7);

        /** Statement canceled error. Statement is canceled due to timeout, admin action, etc. */
        public static final int EXECUTION_CANCELLED_ERR = SQL_ERR_GROUP.registerErrorCode((short) 8);

        /**
         * Runtime error. Errors caused by programming errors in SQL statement itself, such errors happen during statement execution:
         * <ul>
         *     <li>Numeric overflow errors.</li>
         *     <li>Type conversion errors such as {@code SELECT CAST('abc' AS INTEGER)}.</li>
         *     <li>Function execution errors.</li>
         * </ul>
         */
        public static final int RUNTIME_ERR = SQL_ERR_GROUP.registerErrorCode((short) 9);

        /** Planning timed out without finding any valid plan. */
        public static final int PLANNING_TIMEOUT_ERR = SQL_ERR_GROUP.registerErrorCode((short) 10);

        /** Session closed error. Operation is rejected because SQL session was closed. */
        public static final int SESSION_CLOSED_ERR = SQL_ERR_GROUP.registerErrorCode((short) 11);

        /**
         * SQL engine was unable to map query on current cluster topology.
         *
         * <p>This may be due to a variety of reasons, but most probably because of all nodes hosting certain system view
         * or a table partition went offline.
         *
         * <p>See error message for details.
         */
        public static final int MAPPING_ERR = SQL_ERR_GROUP.registerErrorCode((short) 12);
    }

    /** Meta storage error group. */
    public static class MetaStorage {
        /** Meta storage error group. */
        public static final ErrorGroup META_STORAGE_ERR_GROUP = registerGroup("META", (short) 5);

        /** Failed to start the underlying key value storage. */
        public static final int STARTING_STORAGE_ERR = META_STORAGE_ERR_GROUP.registerErrorCode((short) 1);

        /** Failed to restore the underlying key value storage. */
        public static final int RESTORING_STORAGE_ERR = META_STORAGE_ERR_GROUP.registerErrorCode((short) 2);

        /** Failed to compact the underlying key value storage. */
        public static final int COMPACTION_ERR = META_STORAGE_ERR_GROUP.registerErrorCode((short) 3);

        /** Failed to perform an operation on the underlying key value storage. */
        public static final int OP_EXECUTION_ERR = META_STORAGE_ERR_GROUP.registerErrorCode((short) 4);

        /** Failed to perform an operation within a specified time period. Usually in such cases the operation should be retried. */
        public static final int OP_EXECUTION_TIMEOUT_ERR = META_STORAGE_ERR_GROUP.registerErrorCode((short) 5);
    }

    /** Index error group. */
    public static class Index {
        /** Index error group. */
        public static final ErrorGroup INDEX_ERR_GROUP = registerGroup("IDX", (short) 6);

        /** Invalid index definition. */
        public static final int INVALID_INDEX_DEFINITION_ERR = INDEX_ERR_GROUP.registerErrorCode((short) 1);

        /** Index not found. */
        public static final int INDEX_NOT_FOUND_ERR = INDEX_ERR_GROUP.registerErrorCode((short) 2);

        /** Index already exists. */
        public static final int INDEX_ALREADY_EXISTS_ERR = INDEX_ERR_GROUP.registerErrorCode((short) 3);
    }

    /** Transactions error group. */
    public static class Transactions {
        /** Transactions error group. */
        public static final ErrorGroup TX_ERR_GROUP = registerGroup("TX", (short) 7);

        /** Error of tx state storage. */
        public static final int TX_STATE_STORAGE_ERR = TX_ERR_GROUP.registerErrorCode((short) 1);

        /** Tx state storage is stopped. */
        public static final int TX_STATE_STORAGE_STOPPED_ERR = TX_ERR_GROUP.registerErrorCode((short) 2);

        /** Error of unexpected tx state on state change. */
        public static final int TX_UNEXPECTED_STATE_ERR = TX_ERR_GROUP.registerErrorCode((short) 3);

        /** Failed to acquire a lock on a key due to a conflict. */
        public static final int ACQUIRE_LOCK_ERR = TX_ERR_GROUP.registerErrorCode((short) 4);

        /** Failed to acquire a lock on a key within a timeout. */
        public static final int ACQUIRE_LOCK_TIMEOUT_ERR = TX_ERR_GROUP.registerErrorCode((short) 5);

        /** Failed to commit a transaction. */
        public static final int TX_COMMIT_ERR = TX_ERR_GROUP.registerErrorCode((short) 6);

        /** Failed to rollback a transaction. */
        public static final int TX_ROLLBACK_ERR = TX_ERR_GROUP.registerErrorCode((short) 7);

        /** Failed to enlist read-write operation into read-only transaction. */
        public static final int TX_FAILED_READ_WRITE_OPERATION_ERR = TX_ERR_GROUP.registerErrorCode((short) 8);

        /** The error happens when the replica is not ready to handle a request. */
        public static final int TX_REPLICA_UNAVAILABLE_ERR = TX_ERR_GROUP.registerErrorCode((short) 9);

        /** Tx state storage rebalancing error. */
        public static final int TX_STATE_STORAGE_REBALANCE_ERR = TX_ERR_GROUP.registerErrorCode((short) 10);

        /** Error occurred when trying to create a read-only transaction with a timestamp older than the data available in the tables. */
        public static final int TX_READ_ONLY_TOO_OLD_ERR = TX_ERR_GROUP.registerErrorCode((short) 11);

        /** Failure due to an incompatible schema change. */
        public static final int TX_INCOMPATIBLE_SCHEMA_ERR = TX_ERR_GROUP.registerErrorCode((short) 12);

        /** Failure due to an abandoned transaction. */
        public static final int TX_ABANDONED_ERR = TX_ERR_GROUP.registerErrorCode((short) 13);
    }

    /** Replicator error group. */
    public static class Replicator {
        /** Replicator error group. */
        public static final ErrorGroup REPLICATOR_ERR_GROUP = registerGroup("REP", (short) 8);

        /** Common error for the replication procedure. */
        public static final int REPLICA_COMMON_ERR = REPLICATOR_ERR_GROUP.registerErrorCode((short) 1);

        /** Replica with the same identifier is already existed. */
        public static final int REPLICA_IS_ALREADY_STARTED_ERR = REPLICATOR_ERR_GROUP.registerErrorCode((short) 2);

        /** Timeout has happened during the replication procedure. */
        public static final int REPLICA_TIMEOUT_ERR = REPLICATOR_ERR_GROUP.registerErrorCode((short) 3);

        /** The error happens when the replication level try to handle an unsupported request. */
        public static final int REPLICA_UNSUPPORTED_REQUEST_ERR = REPLICATOR_ERR_GROUP.registerErrorCode((short) 4);

        /** The error happens when the replica is not ready to handle a request. */
        public static final int REPLICA_UNAVAILABLE_ERR = REPLICATOR_ERR_GROUP.registerErrorCode((short) 5);

        /** The error happens when the replica is not the current primary replica. */
        public static final int REPLICA_MISS_ERR = REPLICATOR_ERR_GROUP.registerErrorCode((short) 6);

        /** Failed to close cursor. */
        public static final int CURSOR_CLOSE_ERR = REPLICATOR_ERR_GROUP.registerErrorCode((short) 7);

        /** Stopping replica exception code. */
        public static final int REPLICA_STOPPING_ERR = REPLICATOR_ERR_GROUP.registerErrorCode((short) 8);

    }

    /** Storage error group. */
    public static class Storage {
        /** Storage error group. */
        public static final ErrorGroup STORAGE_ERR_GROUP = registerGroup("STORAGE", (short) 9);

        /** Default error code when nothing else is specified. */
        public static final int GENERIC_ERR = STORAGE_ERR_GROUP.registerErrorCode((short) 1);

        /** Failed to create a directory. */
        public static final int DIRECTORY_CREATION_ERR = STORAGE_ERR_GROUP.registerErrorCode((short) 2);

        /** Operation on closed storage. */
        public static final int ALREADY_CLOSED_ERR = STORAGE_ERR_GROUP.registerErrorCode((short) 3);

        /** Storage rebalancing error. */
        public static final int STORAGE_REBALANCE_ERR = STORAGE_ERR_GROUP.registerErrorCode((short) 4);
    }

    /** Distribution zones error group. */
    public static class DistributionZones {
        /** Distribution zones group. */
        public static final ErrorGroup DISTRIBUTION_ZONES_ERR_GROUP = registerGroup("DISTRZONES", (short) 10);

        /** Distribution zone already exists. */
        public static final int ZONE_ALREADY_EXISTS_ERR = DISTRIBUTION_ZONES_ERR_GROUP.registerErrorCode((short) 1);

        /** Distribution zone is not found. */
        public static final int ZONE_NOT_FOUND_ERR = DISTRIBUTION_ZONES_ERR_GROUP.registerErrorCode((short) 2);

        /** Distribution zone rename error. */
        public static final int ZONE_RENAME_ERR = DISTRIBUTION_ZONES_ERR_GROUP.registerErrorCode((short) 3);

        /** Distribution zone is a default distribution zone or bound to table. */
        public static final int ZONE_DROP_ERR = DISTRIBUTION_ZONES_ERR_GROUP.registerErrorCode((short) 4);

        /** Distribution zone definition error. */
        public static final int ZONE_DEFINITION_ERR = DISTRIBUTION_ZONES_ERR_GROUP.registerErrorCode((short) 5);
    }

    /** Network error group. */
    public static class Network {
        /** Network error group. */
        public static final ErrorGroup NETWORK_ERR_GROUP = registerGroup("NETWORK", (short) 11);

        /** Unresolvable consistent ID. */
        public static final int UNRESOLVABLE_CONSISTENT_ID_ERR = NETWORK_ERR_GROUP.registerErrorCode((short) 1);

        /** Port is in use. */
        public static final int PORT_IN_USE_ERR = NETWORK_ERR_GROUP.registerErrorCode((short) 2);
    }

    /** Node configuration error group. */
    public static class NodeConfiguration {
        /** Node configuration error group. */
        public static final ErrorGroup NODE_CONFIGURATION_ERR_GROUP = registerGroup("NODECFG", (short) 12);

        /** Config read error. */
        public static final int CONFIG_READ_ERR = NODE_CONFIGURATION_ERR_GROUP.registerErrorCode((short) 1);

        /** Config file creation error. */
        public static final int CONFIG_FILE_CREATE_ERR = NODE_CONFIGURATION_ERR_GROUP.registerErrorCode((short) 2);

        /** Config write error. */
        public static final int CONFIG_WRITE_ERR = NODE_CONFIGURATION_ERR_GROUP.registerErrorCode((short) 3);

        /** Config parse error. */
        public static final int CONFIG_PARSE_ERR = NODE_CONFIGURATION_ERR_GROUP.registerErrorCode((short) 4);
    }

    /** Code deployment error group. */
    public static class CodeDeployment {
        /** Code deployment error group. */
        public static final ErrorGroup CODE_DEPLOYMENT_ERR_GROUP = registerGroup("CODEDEPLOY", (short) 13);

        /** Access to non-existing deployment unit. */
        public static final int UNIT_NOT_FOUND_ERR = CODE_DEPLOYMENT_ERR_GROUP.registerErrorCode((short) 1);

        /** Unit duplicate error. */
        public static final int UNIT_ALREADY_EXISTS_ERR = CODE_DEPLOYMENT_ERR_GROUP.registerErrorCode((short) 2);

        /** Deployment unit content read error. */
        public static final int UNIT_CONTENT_READ_ERR = CODE_DEPLOYMENT_ERR_GROUP.registerErrorCode((short) 3);

        /** Deployment unit is unavailable for computing. */
        public static final int UNIT_UNAVAILABLE_ERR = CODE_DEPLOYMENT_ERR_GROUP.registerErrorCode((short) 4);
    }

    /**
     * Garbage collector error group.
     */
    public static class GarbageCollector {
        /** Garbage collector error group. */
        public static final ErrorGroup GC_ERR_GROUP = registerGroup("GC", (short) 14);

        /** Garbage collector closed error. */
        public static final int CLOSED_ERR = GC_ERR_GROUP.registerErrorCode((short) 1);
    }

    /**
     * Authentication error group.
     */
    public static class Authentication {
        /** Authentication error group. */
        public static final ErrorGroup AUTHENTICATION_ERR_GROUP = registerGroup("AUTHENTICATION", (short) 15);

        /** General authentication error. */
        public static final int COMMON_AUTHENTICATION_ERR = AUTHENTICATION_ERR_GROUP.registerErrorCode((short) 1);
    }

    /**
     * Compute error group.
     */
    public static class Compute {
        /** Compute error group. */
        public static final ErrorGroup COMPUTE_ERR_GROUP = registerGroup("COMPUTE", (short) 16);

        /** Classpath error. */
        public static final int CLASS_PATH_ERR = COMPUTE_ERR_GROUP.registerErrorCode((short) 1);

        /** Class loader error. */
        public static final int CLASS_LOADER_ERR = COMPUTE_ERR_GROUP.registerErrorCode((short) 2);
    }

    /** Catalog error group. */
    public static class Catalog {
        /** Catalog error group. */
        public static final ErrorGroup CATALOG_ERR_GROUP = registerGroup("CATALOG", (short) 17);

        /** Command to the catalog has not passed the validation. See exception message for details. */
        public static final int VALIDATION_ERR = CATALOG_ERR_GROUP.registerErrorCode((short) 1);
    }

    /** Placement driver error group. */
    public static class PlacementDriver {
        /** Placement driver error group. */
        public static final ErrorGroup PLACEMENT_DRIVER_ERR_GROUP = registerGroup("PLACEMENTDRIVER", (short) 18);

        /** Primary replica await timeout error. */
        public static final int PRIMARY_REPLICA_AWAIT_TIMEOUT_ERR = PLACEMENT_DRIVER_ERR_GROUP.registerErrorCode((short) 1);

        /** Primary replica await error. */
        public static final int PRIMARY_REPLICA_AWAIT_ERR = PLACEMENT_DRIVER_ERR_GROUP.registerErrorCode((short) 2);
    }
}
