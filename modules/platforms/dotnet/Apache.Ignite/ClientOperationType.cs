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

namespace Apache.Ignite
{
    using Compute;
    using Sql;
    using Table;

    /// <summary>
    /// Client operation type.
    /// </summary>
    public enum ClientOperationType
    {
        /// <summary>
        /// Get tables (<see cref="ITables.GetTablesAsync"/>).
        /// </summary>
        TablesGet,

        /// <summary>
        /// Get table (<see cref="ITables.GetTableAsync(string)"/>).
        /// </summary>
        TableGet,

        /// <summary>
        /// Upsert (<see cref="IRecordView{T}.UpsertAsync"/>).
        /// </summary>
        TupleUpsert,

        /// <summary>
        /// Get (<see cref="IRecordView{T}.GetAsync"/>).
        /// </summary>
        TupleGet,

        /// <summary>
        /// Upsert (<see cref="IRecordView{T}.UpsertAllAsync"/>).
        /// </summary>
        TupleUpsertAll,

        /// <summary>
        /// Get All (<see cref="IRecordView{T}.GetAllAsync"/>).
        /// </summary>
        TupleGetAll,

        /// <summary>
        /// Get and Upsert (<see cref="IRecordView{T}.GetAndUpsertAsync"/>).
        /// </summary>
        TupleGetAndUpsert,

        /// <summary>
        /// Insert (<see cref="IRecordView{T}.InsertAsync"/>).
        /// </summary>
        TupleInsert,

        /// <summary>
        /// Insert All (<see cref="IRecordView{T}.InsertAllAsync"/>).
        /// </summary>
        TupleInsertAll,

        /// <summary>
        /// Replace (<see cref="IRecordView{T}.ReplaceAsync(Apache.Ignite.Transactions.ITransaction?,T)"/>).
        /// </summary>
        TupleReplace,

        /// <summary>
        /// Replace Exact (<see cref="IRecordView{T}.ReplaceAsync(Apache.Ignite.Transactions.ITransaction?,T, T)"/>).
        /// </summary>
        TupleReplaceExact,

        /// <summary>
        /// Get and Replace (<see cref="IRecordView{T}.GetAndReplaceAsync"/>).
        /// </summary>
        TupleGetAndReplace,

        /// <summary>
        /// Delete (<see cref="IRecordView{T}.DeleteAsync"/>).
        /// </summary>
        TupleDelete,

        /// <summary>
        /// Delete All (<see cref="IRecordView{T}.DeleteAllAsync"/>).
        /// </summary>
        TupleDeleteAll,

        /// <summary>
        /// Delete Exact (<see cref="IRecordView{T}.DeleteExactAsync"/>).
        /// </summary>
        TupleDeleteExact,

        /// <summary>
        /// Delete All Exact (<see cref="IRecordView{T}.DeleteAllExactAsync"/>).
        /// </summary>
        TupleDeleteAllExact,

        /// <summary>
        /// Get and Delete (<see cref="IRecordView{T}.GetAndDeleteAsync"/>).
        /// </summary>
        TupleGetAndDelete,

        /// <summary>
        /// Contains key (<see cref="IKeyValueView{TK,TV}.ContainsAsync"/>).
        /// </summary>
        TupleContainsKey,

        /// <summary>
        /// Compute (<see cref="ICompute"/>).
        /// </summary>
        ComputeExecute,

        /// <summary>
        /// Compute (<see cref="ICompute"/>).
        /// </summary>
        ComputeExecuteMapReduce,

        /// <summary>
        /// SQL (<see cref="ISql"/>).
        /// </summary>
        SqlExecute,

        /// <summary>
        /// SQL script (<see cref="ISql"/>).
        /// </summary>
        SqlExecuteScript,

        /// <summary>
        /// Get status of a compute job (<see cref="IJobExecution{T}.GetStateAsync"/>).
        /// </summary>
        ComputeGetStatus,

        /// <summary>
        /// Cancel compute job.
        /// </summary>
        ComputeCancel,

        /// <summary>
        /// Change compute job priority (<see cref="IJobExecution{T}.ChangePriorityAsync"/>).
        /// </summary>
        ComputeChangePriority,

        /// <summary>
        /// Get primary replicas (<see cref="IPartitionManager.GetPrimaryReplicasAsync"/>).
        /// </summary>
        PrimaryReplicasGet,

        /// <summary>
        /// Send data streamer batch (<see cref="IDataStreamerTarget{T}"/>).
        /// </summary>
        StreamerBatchSend,

        /// <summary>
        /// Send data streamer batch with receiver (<see cref="IDataStreamerTarget{T}"/>).
        /// </summary>
        StreamerWithReceiverBatchSend,

        /// <summary>
        /// SQL batch (<see cref="ISql.ExecuteBatchAsync"/>).
        /// </summary>
        SqlExecuteBatch
    }
}
