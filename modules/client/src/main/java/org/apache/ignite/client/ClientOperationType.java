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

package org.apache.ignite.client;

import java.util.Collection;
import java.util.List;
import org.apache.ignite.compute.JobDescriptor;
import org.apache.ignite.compute.JobTarget;
import org.apache.ignite.sql.BatchedArguments;
import org.apache.ignite.sql.IgniteSql;
import org.apache.ignite.sql.async.AsyncResultSet;
import org.apache.ignite.table.DataStreamerTarget;
import org.apache.ignite.table.IgniteTables;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.partition.PartitionManager;
import org.apache.ignite.tx.Transaction;

/**
 * Client operation type.
 */
public enum ClientOperationType {
    /**
     * Connect channel.
     */
    CHANNEL_CONNECT,

    /**
     * Get tables ({@link IgniteTables#tables()}).
     */
    TABLES_GET,

    /**
     * Get table ({@link IgniteTables#table(String)}).
     */
    TABLE_GET,

    /**
     * Upsert ({@link RecordView#upsert(Transaction, Object)}).
     */
    TUPLE_UPSERT,

    /**
     * Get ({@link RecordView#get(Transaction, Object)}).
     */
    TUPLE_GET,

    /**
     * Upsert ({@link RecordView#upsertAll(Transaction, Collection)}).
     */
    TUPLE_UPSERT_ALL,

    /**
     * Get All ({@link RecordView#getAll(Transaction, Collection)}).
     */
    TUPLE_GET_ALL,

    /**
     * Get and Upsert ({@link RecordView#getAndUpsert(Transaction, Object)}).
     */
    TUPLE_GET_AND_UPSERT,

    /**
     * Insert ({@link RecordView#insert(Transaction, Object)}).
     */
    TUPLE_INSERT,

    /**
     * Insert All ({@link RecordView#insertAll(Transaction, Collection)}).
     */
    TUPLE_INSERT_ALL,

    /**
     * Replace ({@link RecordView#replace(Transaction, Object)}).
     */
    TUPLE_REPLACE,

    /**
     * Replace Exact ({@link RecordView#replace(Transaction, Object, Object)}).
     */
    TUPLE_REPLACE_EXACT,

    /**
     * Get and Replace ({@link RecordView#getAndReplace(Transaction, Object)}).
     */
    TUPLE_GET_AND_REPLACE,

    /**
     * Delete ({@link RecordView#delete(Transaction, Object)}).
     */
    TUPLE_DELETE,

    /**
     * Delete All ({@link RecordView#deleteAll(Transaction, Collection)}).
     */
    TUPLE_DELETE_ALL,

    /**
     * Delete Exact ({@link RecordView#deleteExact(Transaction, Object)}).
     */
    TUPLE_DELETE_EXACT,

    /**
     * Delete All Exact ({@link RecordView#deleteAllExact(Transaction, Collection)}).
     */
    TUPLE_DELETE_ALL_EXACT,

    /**
     * Get and Delete ({@link RecordView#getAndDelete(Transaction, Object)}).
     */
    TUPLE_GET_AND_DELETE,

    /**
     * Contains Key ({@link org.apache.ignite.table.KeyValueView#contains(Transaction, Object)}).
     */
    TUPLE_CONTAINS_KEY,

    /**
     * Compute Execute ({@link org.apache.ignite.compute.IgniteCompute#submit(JobTarget, JobDescriptor, Object...)}).
     */
    COMPUTE_EXECUTE,

    /**
     * Compute Execute MapReduce ({@link org.apache.ignite.compute.IgniteCompute#submitMapReduce(List, String, Object...)}).
     */
    COMPUTE_EXECUTE_MAPREDUCE,

    /**
     * Get compute job status ({@link org.apache.ignite.compute.JobExecution#statusAsync()}).
     */
    COMPUTE_GET_STATUS,

    /**
     * Cancel compute job ({@link org.apache.ignite.compute.JobExecution#cancelAsync()}).
     */
    COMPUTE_CANCEL,

    /**
     * Change compute job priority ({@link org.apache.ignite.compute.JobExecution#changePriorityAsync(int)}).
     */
    COMPUTE_CHANGE_PRIORITY,

    /**
     * SQL Execute ({@link IgniteSql#executeAsync(Transaction, String, Object...)}).
     */
    SQL_EXECUTE,

    /**
     * SQL Execute ({@link IgniteSql#executeScriptAsync(String, Object...)}).
     */
    SQL_EXECUTE_SCRIPT,

    /**
     * SQL Cursor Next Page ({@link AsyncResultSet#fetchNextPage()}).
     */
    SQL_CURSOR_NEXT_PAGE,

    /**
     * Send streamer batch ({@link DataStreamerTarget#streamData}).
     */
    STREAMER_BATCH_SEND,

    /**
     * Send streamer batch with receiver ({@link DataStreamerTarget#streamData}).
     */
    STREAMER_WITH_RECEIVER_BATCH_SEND,

    /**
     * SQL Execute batch ({@link IgniteSql#executeBatchAsync(Transaction, String, BatchedArguments)}).
     */
    SQL_EXECUTE_BATCH,

    /**
     * Get all primary replicas mapping to cluster nodes ({@link PartitionManager#primaryReplicasAsync()}).
     */
    PRIMARY_REPLICAS_GET
}
