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
import org.apache.ignite.error.code.annotations.ErrorCodeGroup;

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
     * @throws IllegalArgumentException If the specified name or group code already registered. Also, this exception is thrown if
     *         the given {@code groupName} is {@code null} or empty.
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
        ErrorGroup grp = registeredGroups.get(extractGroupCode(code));
        assert grp != null : "group not found, code=" + code;
        return grp;
    }

    /** Common error group. */
    @ErrorCodeGroup
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

        /** Cursor is already closed error. */
        public static final int CURSOR_ALREADY_CLOSED_ERR = COMMON_ERR_GROUP.registerErrorCode((short) 6);

        /** Resource closing error. */
        public static final int RESOURCE_CLOSING_ERR = COMMON_ERR_GROUP.registerErrorCode((short) 7);

        /** Can't marshal/unmarshal a user object. */
        public static final int USER_OBJECT_SERIALIZATION_ERR = COMMON_ERR_GROUP.registerErrorCode((short) 8);

        /**
         * This error code indicates that a method can't return a {@code null} value due it's ambiguity
         * (whether the value is absent or is {@code null}).
         **/
        public static final int NULLABLE_VALUE_ERR = COMMON_ERR_GROUP.registerErrorCode((short) 9);

        /**
         * This error code represents an internal error caused by faulty logic or coding in the Ignite codebase. In general, this error code
         * should be considered as a non-recoverable error
         */
        public static final int INTERNAL_ERR = COMMON_ERR_GROUP.registerErrorCode((short) 0xFFFF);
    }

    /** Tables error group. */
    @ErrorCodeGroup
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

        /** Unsupported partition type. */
        public static final int UNSUPPORTED_PARTITION_TYPE_ERR = TABLE_ERR_GROUP.registerErrorCode((short) 8);
    }

    /** Client error group. */
    @ErrorCodeGroup
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
    @ErrorCodeGroup
    public static class Sql {
        /** SQL error group. */
        public static final ErrorGroup SQL_ERR_GROUP = registerGroup("SQL", (short) 4);

        /** Query without a result set error. */
        public static final int QUERY_NO_RESULT_SET_ERR = SQL_ERR_GROUP.registerErrorCode((short) 1);

        /** Schema not found. */
        public static final int SCHEMA_NOT_FOUND_ERR = SQL_ERR_GROUP.registerErrorCode((short) 2);

        /** Statement parsing error. This error is returned when an SQL statement string is not valid according to syntax rules. */
        public static final int STMT_PARSE_ERR = SQL_ERR_GROUP.registerErrorCode((short) 3);

        /**
         * Statement validation error. Although statement is grammatically correct, the semantic is in question. This error may appear in
         * following cases:
         * <ul>
         *     <li>the statement refer to relation that doesn't exists.</li>
         *     <li>the statement describes action that is prohibited by the system, like changing columns belonging to primary keys.</li>
         *     <li>the statement contains operation that is not defined for given operands' types, like addition of DATE and DECIMAL.</li>
         *     <li>etc</li>
         * </ul>
         *
         * <p>See message for details.
         */
        public static final int STMT_VALIDATION_ERR = SQL_ERR_GROUP.registerErrorCode((short) 4);

        /** Constraint violation error such as primary key violation. */
        public static final int CONSTRAINT_VIOLATION_ERR = SQL_ERR_GROUP.registerErrorCode((short) 5);

        /** Statement canceled error. Statement is canceled due to timeout, admin action, etc. */
        public static final int EXECUTION_CANCELLED_ERR = SQL_ERR_GROUP.registerErrorCode((short) 6);

        /**
         * Runtime error. Errors caused by programming errors in SQL statement itself, such errors happen during statement execution:
         * <ul>
         *     <li>Numeric overflow errors.</li>
         *     <li>Type conversion errors such as {@code SELECT CAST('abc' AS INTEGER)}.</li>
         *     <li>Function execution errors.</li>
         * </ul>
         */
        public static final int RUNTIME_ERR = SQL_ERR_GROUP.registerErrorCode((short) 7);

        /**
         * SQL engine was unable to map query on current cluster topology.
         *
         * <p>This may be due to a variety of reasons, but most probably because of all nodes hosting certain system view
         * or a table partition went offline.
         *
         * <p>See error message for details.
         */
        public static final int MAPPING_ERR = SQL_ERR_GROUP.registerErrorCode((short) 8);

        /** Execution of transaction control statement inside an external transaction is forbidden. */
        public static final int TX_CONTROL_INSIDE_EXTERNAL_TX_ERR = SQL_ERR_GROUP.registerErrorCode((short) 9);
    }

    /** Meta storage error group. */
    @ErrorCodeGroup
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
    @ErrorCodeGroup
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
    @ErrorCodeGroup
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

        /** Failure due to primary replica expiration. */
        public static final int TX_PRIMARY_REPLICA_EXPIRED_ERR = TX_ERR_GROUP.registerErrorCode((short) 13);

        /** Operation failed because the transaction is already finished. */
        public static final int TX_ALREADY_FINISHED_ERR = TX_ERR_GROUP.registerErrorCode((short) 14);

        /** Failure due to a stale operation of a completed transaction is detected. */
        public static final int TX_STALE_OPERATION_ERR = TX_ERR_GROUP.registerErrorCode((short) 15);
    }

    /** Replicator error group. */
    @ErrorCodeGroup
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

        /** Replication safe time reordering. */
        public static final int REPLICATION_SAFE_TIME_REORDERING_ERR = REPLICATOR_ERR_GROUP.registerErrorCode((short) 9);
    }

    /** Storage error group. */
    @ErrorCodeGroup
    public static class Storage {
        /** Storage error group. */
        public static final ErrorGroup STORAGE_ERR_GROUP = registerGroup("STORAGE", (short) 9);

        /** Default error code when nothing else is specified. */
        public static final int GENERIC_ERR = STORAGE_ERR_GROUP.registerErrorCode((short) 1);

        /** Failed to create a directory. */
        public static final int DIRECTORY_CREATION_ERR = STORAGE_ERR_GROUP.registerErrorCode((short) 2);

        /** Operation on a closed storage. */
        public static final int ALREADY_CLOSED_ERR = STORAGE_ERR_GROUP.registerErrorCode((short) 3);

        /** Storage rebalancing error. */
        public static final int STORAGE_REBALANCE_ERR = STORAGE_ERR_GROUP.registerErrorCode((short) 4);

        /** Operation on a destroyed storage. */
        public static final int ALREADY_DESTROYED_ERR = STORAGE_ERR_GROUP.registerErrorCode((short) 5);

        /** Error reading from an index that has not yet been built. */
        public static final int INDEX_NOT_BUILT_ERR = STORAGE_ERR_GROUP.registerErrorCode((short) 6);
    }

    /** Distribution zones error group. */
    @ErrorCodeGroup
    public static class DistributionZones {
        /** Distribution zones group. */
        public static final ErrorGroup DISTRIBUTION_ZONES_ERR_GROUP = registerGroup("DISTRZONES", (short) 10);

        /** Distribution zone was not found. */
        public static final int ZONE_NOT_FOUND_ERR = DISTRIBUTION_ZONES_ERR_GROUP.registerErrorCode((short) 1);
    }

    /** Network error group. */
    @ErrorCodeGroup
    public static class Network {
        /** Network error group. */
        public static final ErrorGroup NETWORK_ERR_GROUP = registerGroup("NETWORK", (short) 11);

        /** Unresolvable consistent ID. */
        public static final int UNRESOLVABLE_CONSISTENT_ID_ERR = NETWORK_ERR_GROUP.registerErrorCode((short) 1);

        /** Port is in use. */
        public static final int PORT_IN_USE_ERR = NETWORK_ERR_GROUP.registerErrorCode((short) 2);

        /** Recipient node has left the physical topology. */
        public static final int RECIPIENT_LEFT_ERR = NETWORK_ERR_GROUP.registerErrorCode((short) 5);

        /** Could not resolve address. */
        public static final int ADDRESS_UNRESOLVED_ERR = NETWORK_ERR_GROUP.registerErrorCode((short) 6);
    }

    /** Node configuration error group. */
    @ErrorCodeGroup
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
    @ErrorCodeGroup
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
    @ErrorCodeGroup
    public static class GarbageCollector {
        /** Garbage collector error group. */
        public static final ErrorGroup GC_ERR_GROUP = registerGroup("GC", (short) 14);

        /** Garbage collector closed error. */
        public static final int CLOSED_ERR = GC_ERR_GROUP.registerErrorCode((short) 1);
    }

    /**
     * Authentication error group.
     */
    @ErrorCodeGroup
    public static class Authentication {
        /** Authentication error group. */
        public static final ErrorGroup AUTHENTICATION_ERR_GROUP = registerGroup("AUTHENTICATION", (short) 15);

        /** Authentication error caused by unsupported authentication type. */
        public static final int UNSUPPORTED_AUTHENTICATION_TYPE_ERR = AUTHENTICATION_ERR_GROUP.registerErrorCode((short) 1);

        /** Authentication error caused by invalid credentials. */
        public static final int INVALID_CREDENTIALS_ERR = AUTHENTICATION_ERR_GROUP.registerErrorCode((short) 2);

        /** Basic authentication provider is not found. */
        public static final int BASIC_PROVIDER_ERR = AUTHENTICATION_ERR_GROUP.registerErrorCode((short) 3);
    }

    /**
     * Compute error group.
     */
    @ErrorCodeGroup
    public static class Compute {
        /** Compute error group. */
        public static final ErrorGroup COMPUTE_ERR_GROUP = registerGroup("COMPUTE", (short) 16);

        /** Classpath error. */
        public static final int CLASS_PATH_ERR = COMPUTE_ERR_GROUP.registerErrorCode((short) 1);

        /** Class loader error. */
        public static final int CLASS_LOADER_ERR = COMPUTE_ERR_GROUP.registerErrorCode((short) 2);

        /** Job class initialization error. */
        public static final int CLASS_INITIALIZATION_ERR = COMPUTE_ERR_GROUP.registerErrorCode((short) 3);

        /** Compute execution queue overflow error. */
        public static final int QUEUE_OVERFLOW_ERR = COMPUTE_ERR_GROUP.registerErrorCode((short) 4);

        /** Compute job status transition error. */
        public static final int COMPUTE_JOB_STATUS_TRANSITION_ERR = COMPUTE_ERR_GROUP.registerErrorCode((short) 5);

        /** Compute job cancel failed error. */
        public static final int CANCELLING_ERR = COMPUTE_ERR_GROUP.registerErrorCode((short) 6);

        /** Compute job result not found error. */
        public static final int RESULT_NOT_FOUND_ERR = COMPUTE_ERR_GROUP.registerErrorCode((short) 7);

        /** Compute job state can't be retrieved. */
        public static final int FAIL_TO_GET_JOB_STATE_ERR = COMPUTE_ERR_GROUP.registerErrorCode((short) 8);

        /** Compute job failed. */
        public static final int COMPUTE_JOB_FAILED_ERR = COMPUTE_ERR_GROUP.registerErrorCode((short) 9);

        /** Cannot change job priority, compute job not found error. */
        public static final int CHANGE_JOB_PRIORITY_NO_JOB_ERR = COMPUTE_ERR_GROUP.registerErrorCode((short) 10);

        /** Cannot change job priority, compute job is already executing. */
        public static final int CHANGE_JOB_PRIORITY_JOB_EXECUTING_ERR = COMPUTE_ERR_GROUP.registerErrorCode((short) 11);

        /** Cannot resolve primary replica for colocated execution. */
        public static final int PRIMARY_REPLICA_RESOLVE_ERR = COMPUTE_ERR_GROUP.registerErrorCode((short) 12);

        /** Cannot change job priority. */
        public static final int CHANGE_JOB_PRIORITY_ERR = COMPUTE_ERR_GROUP.registerErrorCode((short) 13);

        /** Specified node is not found in the cluster. */
        public static final int NODE_NOT_FOUND_ERR = COMPUTE_ERR_GROUP.registerErrorCode((short) 14);
    }

    /** Catalog error group. */
    @ErrorCodeGroup
    public static class Catalog {
        /** Catalog error group. */
        public static final ErrorGroup CATALOG_ERR_GROUP = registerGroup("CATALOG", (short) 17);

        /** Command to the catalog has not passed the validation. See exception message for details. */
        public static final int VALIDATION_ERR = CATALOG_ERR_GROUP.registerErrorCode((short) 1);
    }

    /** Placement driver error group. */
    @ErrorCodeGroup
    public static class PlacementDriver {
        /** Placement driver error group. */
        public static final ErrorGroup PLACEMENT_DRIVER_ERR_GROUP = registerGroup("PLACEMENTDRIVER", (short) 18);

        /** Primary replica await timeout error. */
        public static final int PRIMARY_REPLICA_AWAIT_TIMEOUT_ERR = PLACEMENT_DRIVER_ERR_GROUP.registerErrorCode((short) 1);

        /** Primary replica await error. */
        public static final int PRIMARY_REPLICA_AWAIT_ERR = PLACEMENT_DRIVER_ERR_GROUP.registerErrorCode((short) 2);
    }

    /** Critical workers error group. */
    @ErrorCodeGroup
    public static class CriticalWorkers {
        /** Critical workers error group. */
        public static final ErrorGroup CRITICAL_WORKERS_ERR_GROUP = registerGroup("WORKERS", (short) 19);

        /** System worker does not update its heartbeat for a long time. */
        public static final int SYSTEM_WORKER_BLOCKED_ERR = CRITICAL_WORKERS_ERR_GROUP.registerErrorCode((short) 1);

        /** System-critical operation timed out. */
        public static final int SYSTEM_CRITICAL_OPERATION_TIMEOUT_ERR = CRITICAL_WORKERS_ERR_GROUP.registerErrorCode((short) 2);
    }

    /** Disaster recovery error group. */
    @ErrorCodeGroup
    public static class DisasterRecovery {
        /** Disaster recovery group. */
        public static final ErrorGroup RECOVERY_ERR_GROUP = registerGroup("RECOVERY", (short) 20);

        /** Partition ID is not in valid range. */
        public static final int ILLEGAL_PARTITION_ID_ERR = RECOVERY_ERR_GROUP.registerErrorCode((short) 1);

        /** Nodes were not found. */
        public static final int NODES_NOT_FOUND_ERR = RECOVERY_ERR_GROUP.registerErrorCode((short) 2);

        /** Error while returning partition states. */
        public static final int PARTITION_STATE_ERR = RECOVERY_ERR_GROUP.registerErrorCode((short) 3);

        /** Error while returning partition states. */
        public static final int CLUSTER_NOT_IDLE_ERR = RECOVERY_ERR_GROUP.registerErrorCode((short) 4);
    }

    /** Embedded API error group. */
    @ErrorCodeGroup
    public static class Embedded {
        /** Embedded API group. */
        public static final ErrorGroup EMBEDDED_ERR_GROUP = registerGroup("EMBEDDED", (short) 21);

        /** Cluster is not yet initialized. */
        public static final int CLUSTER_NOT_INITIALIZED_ERR = EMBEDDED_ERR_GROUP.registerErrorCode((short) 1);

        /** Cluster initialization failed. */
        public static final int CLUSTER_INIT_FAILED_ERR = EMBEDDED_ERR_GROUP.registerErrorCode((short) 2);

        /** Node not started. */
        public static final int NODE_NOT_STARTED_ERR = EMBEDDED_ERR_GROUP.registerErrorCode((short) 3);

        /** Node start failed.. */
        public static final int NODE_START_ERR = EMBEDDED_ERR_GROUP.registerErrorCode((short) 4);
    }

    /** Marshalling error group. */
    @ErrorCodeGroup
    public static class Marshalling {
        /** Marshalling error group. */
        public static final ErrorGroup MARSHALLING_ERR_GROUP = registerGroup("MARSHALLING", (short) 22);

        /** Marshalling error. */
        public static final int COMMON_ERR = MARSHALLING_ERR_GROUP.registerErrorCode((short) 1);

        /** Unsupported object type error. */
        public static final int UNSUPPORTED_OBJECT_TYPE_ERR = MARSHALLING_ERR_GROUP.registerErrorCode((short) 2);
    }
}
