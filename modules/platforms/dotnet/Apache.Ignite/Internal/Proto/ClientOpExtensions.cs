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

namespace Apache.Ignite.Internal.Proto
{
    using System;

    /// <summary>
    /// Extensions for <see cref="ClientOp"/>.
    /// </summary>
    internal static class ClientOpExtensions
    {
        /// <summary>
        /// Converts the internal op code to a public operation type.
        /// </summary>
        /// <param name="op">Operation code.</param>
        /// <returns>Operation type.</returns>
        public static ClientOperationType? ToPublicOperationType(this ClientOp op)
        {
            return op switch
            {
                ClientOp.TablesGet => ClientOperationType.TablesGet,
                ClientOp.TableGet => ClientOperationType.TableGet,
                ClientOp.SchemasGet => null,
                ClientOp.TupleUpsert => ClientOperationType.TupleUpsert,
                ClientOp.TupleGet => ClientOperationType.TupleGet,
                ClientOp.TupleUpsertAll => ClientOperationType.TupleUpsertAll,
                ClientOp.TupleGetAll => ClientOperationType.TupleGetAll,
                ClientOp.TupleGetAndUpsert => ClientOperationType.TupleGetAndUpsert,
                ClientOp.TupleInsert => ClientOperationType.TupleInsert,
                ClientOp.TupleInsertAll => ClientOperationType.TupleInsertAll,
                ClientOp.TupleReplace => ClientOperationType.TupleReplace,
                ClientOp.TupleReplaceExact => ClientOperationType.TupleReplaceExact,
                ClientOp.TupleGetAndReplace => ClientOperationType.TupleGetAndReplace,
                ClientOp.TupleDelete => ClientOperationType.TupleDelete,
                ClientOp.TupleDeleteAll => ClientOperationType.TupleDeleteAll,
                ClientOp.TupleDeleteExact => ClientOperationType.TupleDeleteExact,
                ClientOp.TupleDeleteAllExact => ClientOperationType.TupleDeleteAllExact,
                ClientOp.TupleGetAndDelete => ClientOperationType.TupleGetAndDelete,
                ClientOp.TupleContainsKey => ClientOperationType.TupleContainsKey,
                ClientOp.ComputeExecute => ClientOperationType.ComputeExecute,
                ClientOp.ComputeExecuteColocated => ClientOperationType.ComputeExecute,
                ClientOp.ComputeGetStatus => ClientOperationType.ComputeGetStatus,
                ClientOp.ComputeCancel => ClientOperationType.ComputeCancel,
                ClientOp.ComputeChangePriority => ClientOperationType.ComputeChangePriority,
                ClientOp.SqlExec => ClientOperationType.SqlExecute,
                ClientOp.SqlExecScript => ClientOperationType.SqlExecuteScript,
                ClientOp.SqlCursorNextPage => null,
                ClientOp.SqlCursorClose => null,
                ClientOp.TxBegin => null,
                ClientOp.TxCommit => null,
                ClientOp.TxRollback => null,
                ClientOp.Heartbeat => null,
                ClientOp.ClusterGetNodes => null,
                ClientOp.PartitionAssignmentGet => null,
                ClientOp.SqlParamMeta => null,
                ClientOp.StreamerBatchSend => ClientOperationType.StreamerBatchSend,

                // Do not return null from default arm intentionally so we don't forget to update this when new ClientOp values are added.
                // ReSharper disable once PatternIsRedundant
                ClientOp.None or _ => throw new ArgumentOutOfRangeException(nameof(op), op, message: null)
            };
        }
    }
}
