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

namespace Apache.Ignite.Internal.Compute
{
    using System;
    using System.Buffers.Binary;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Diagnostics.CodeAnalysis;
    using System.Linq;
    using System.Threading.Tasks;
    using Buffers;
    using Common;
    using Ignite.Compute;
    using Ignite.Network;
    using Ignite.Table;
    using Proto;
    using Proto.MsgPack;
    using Table;
    using Table.Serialization;
    using TaskStatus = Ignite.Compute.TaskStatus;

    /// <summary>
    /// Compute API.
    /// </summary>
    internal sealed class Compute : ICompute
    {
        /** Socket. */
        private readonly ClientFailoverSocket _socket;

        /** Tables. */
        private readonly Tables _tables;

        /** Cached tables. */
        private readonly ConcurrentDictionary<string, Table> _tableCache = new();

        /// <summary>
        /// Initializes a new instance of the <see cref="Compute"/> class.
        /// </summary>
        /// <param name="socket">Socket.</param>
        /// <param name="tables">Tables.</param>
        public Compute(ClientFailoverSocket socket, Tables tables)
        {
            _socket = socket;
            _tables = tables;
        }

        /// <inheritdoc/>
        public Task<IJobExecution<TResult>> SubmitAsync<TTarget, TArg, TResult>(
            IJobTarget<TTarget> target,
            JobDescriptor<TArg, TResult> jobDescriptor,
            TArg arg)
            where TTarget : notnull
        {
            IgniteArgumentCheck.NotNull(target);
            IgniteArgumentCheck.NotNull(jobDescriptor);

            return target switch
            {
                JobTarget.SingleNodeTarget singleNode => SubmitAsync(new[] { singleNode.Data }, jobDescriptor, arg),
                JobTarget.AnyNodeTarget anyNode => SubmitAsync(anyNode.Data, jobDescriptor, arg),
                JobTarget.ColocatedTarget<TTarget> colocated => SubmitColocatedAsync(colocated, jobDescriptor, arg),

                _ => throw new ArgumentException("Unsupported job target: " + target)
            };
        }

        /// <inheritdoc/>
        public IDictionary<IClusterNode, Task<IJobExecution<TResult>>> SubmitBroadcast<TArg, TResult>(
            IEnumerable<IClusterNode> nodes,
            JobDescriptor<TArg, TResult> jobDescriptor,
            TArg arg)
        {
            IgniteArgumentCheck.NotNull(nodes);
            IgniteArgumentCheck.NotNull(jobDescriptor);
            IgniteArgumentCheck.NotNull(jobDescriptor.JobClassName);

            var res = new Dictionary<IClusterNode, Task<IJobExecution<TResult>>>();

            foreach (var node in nodes)
            {
                // TODO: Remove array allocation.
                Task<IJobExecution<TResult>> task = ExecuteOnNodes(new[] { node }, jobDescriptor, arg);
                res[node] = task;
            }

            return res;
        }

        /// <inheritdoc/>
        public async Task<ITaskExecution<TResult>> SubmitMapReduceAsync<TArg, TResult>(
            TaskDescriptor<TArg, TResult> taskDescriptor,
            TArg arg)
        {
            IgniteArgumentCheck.NotNull(taskDescriptor);
            IgniteArgumentCheck.NotNull(taskDescriptor.TaskClassName);

            using var writer = ProtoCommon.GetMessageWriter();
            Write();

            using PooledBuffer res = await _socket.DoOutInOpAsync(
                    ClientOp.ComputeExecuteMapReduce, writer, expectNotifications: true)
                .ConfigureAwait(false);

            return GetTaskExecution<TResult>(res);

            void Write()
            {
                var w = writer.MessageWriter;

                WriteUnits(taskDescriptor.DeploymentUnits, writer);
                w.Write(taskDescriptor.TaskClassName);

                w.WriteObjectAsBinaryTuple(arg);
            }
        }

        /// <inheritdoc/>
        public override string ToString() => IgniteToStringBuilder.Build(GetType());

        /// <summary>
        /// Writes the deployment units.
        /// </summary>
        /// <param name="units">Units.</param>
        /// <param name="buf">Buffer.</param>
        internal static void WriteUnits(IEnumerable<DeploymentUnit>? units, PooledArrayBuffer buf)
        {
            if (units == null)
            {
                buf.MessageWriter.Write(0);
                return;
            }

            WriteEnumerable(units, buf, writerFunc: unit =>
            {
                IgniteArgumentCheck.NotNullOrEmpty(unit.Name);
                IgniteArgumentCheck.NotNullOrEmpty(unit.Version);

                var w = buf.MessageWriter;
                w.Write(unit.Name);
                w.Write(unit.Version);
            });
        }

        /// <summary>
        /// Gets the job state.
        /// </summary>
        /// <param name="jobId">Job ID.</param>
        /// <returns>State.</returns>
        internal async Task<JobState?> GetJobStateAsync(Guid jobId)
        {
            using var writer = ProtoCommon.GetMessageWriter();
            writer.MessageWriter.Write(jobId);

            using var res = await _socket.DoOutInOpAsync(ClientOp.ComputeGetStatus, writer).ConfigureAwait(false);
            return Read(res.GetReader());

            JobState? Read(MsgPackReader reader) => reader.TryReadNil() ? null : ReadJobState(reader);
        }

        /// <summary>
        /// Gets the task state.
        /// </summary>
        /// <param name="jobId">Job ID.</param>
        /// <returns>State.</returns>
        internal async Task<TaskState?> GetTaskStateAsync(Guid jobId)
        {
            using var writer = ProtoCommon.GetMessageWriter();
            writer.MessageWriter.Write(jobId);

            using var res = await _socket.DoOutInOpAsync(ClientOp.ComputeGetStatus, writer).ConfigureAwait(false);
            return Read(res.GetReader());

            TaskState? Read(MsgPackReader reader) => reader.TryReadNil() ? null : ReadTaskState(reader);
        }

        /// <summary>
        /// Cancels the job.
        /// </summary>
        /// <param name="jobId">Job id.</param>
        /// <returns>
        /// <c>true</c> when the job is cancelled, <c>false</c> when the job couldn't be cancelled
        /// (either it's not yet started, or it's already completed), or <c> null</c> if there's no job with the specified id.
        /// </returns>
        internal async Task<bool?> CancelJobAsync(Guid jobId)
        {
            using var writer = ProtoCommon.GetMessageWriter();
            writer.MessageWriter.Write(jobId);

            using var res = await _socket.DoOutInOpAsync(ClientOp.ComputeCancel, writer).ConfigureAwait(false);
            return res.GetReader().ReadBooleanNullable();
        }

        /// <summary>
        /// Changes the job priority. After priority change the job will be the last in the queue of jobs with the same priority.
        /// </summary>
        /// <param name="jobId">Job id.</param>
        /// <param name="priority">New priority.</param>
        /// <returns>
        /// Returns <c>true</c> if the priority was successfully changed,
        /// <c>false</c> when the priority couldn't be changed (job is already executing or completed),
        /// <c>null</c> if the job was not found (no longer exists due to exceeding the retention time limit).
        /// </returns>
        internal async Task<bool?> ChangeJobPriorityAsync(Guid jobId, int priority)
        {
            using var writer = ProtoCommon.GetMessageWriter();
            writer.MessageWriter.Write(jobId);
            writer.MessageWriter.Write(priority);

            using var res = await _socket.DoOutInOpAsync(ClientOp.ComputeChangePriority, writer).ConfigureAwait(false);
            return res.GetReader().ReadBooleanNullable();
        }

        [SuppressMessage("Security", "CA5394:Do not use insecure randomness", Justification = "Secure random is not required here.")]
        private static IClusterNode GetRandomNode(ICollection<IClusterNode> nodes)
        {
            var idx = Random.Shared.Next(0, nodes.Count);

            return nodes.ElementAt(idx);
        }

        private static ICollection<IClusterNode> GetNodesCollection(IEnumerable<IClusterNode> nodes) =>
            nodes as ICollection<IClusterNode> ?? nodes.ToList();

        private static ICollection<DeploymentUnit> GetUnitsCollection(IEnumerable<DeploymentUnit>? units) =>
            units switch
            {
                null => Array.Empty<DeploymentUnit>(),
                ICollection<DeploymentUnit> c => c,
                var u => u.ToList()
            };

        private static void WriteEnumerable<T>(IEnumerable<T> items, PooledArrayBuffer buf, Action<T> writerFunc)
        {
            var w = buf.MessageWriter;

            if (items.TryGetNonEnumeratedCount(out var count))
            {
                w.Write(count);
                foreach (var item in items)
                {
                    writerFunc(item);
                }

                return;
            }

            // Enumerable without known count - enumerate first, write count later.
            count = 0;
            var countSpan = buf.GetSpan(5);
            buf.Advance(5);

            foreach (var item in items)
            {
                count++;
                writerFunc(item);
            }

            countSpan[0] = MsgPackCode.Array32;
            BinaryPrimitives.WriteInt32BigEndian(countSpan[1..], count);
        }

        private static void WriteNodeNames(IEnumerable<IClusterNode> nodes, PooledArrayBuffer buf)
        {
            WriteEnumerable(nodes, buf, writerFunc: node =>
            {
                var w = buf.MessageWriter;
                w.Write(node.Name);
            });
        }

        private static JobState ReadJobState(MsgPackReader reader)
        {
            var id = reader.ReadGuid();
            var state = (JobStatus)reader.ReadInt32();
            var createTime = reader.ReadInstantNullable();
            var startTime = reader.ReadInstantNullable();
            var endTime = reader.ReadInstantNullable();

            return new JobState(id, state, createTime.GetValueOrDefault(), startTime, endTime);
        }

        private static TaskState ReadTaskState(MsgPackReader reader)
        {
            var id = reader.ReadGuid();
            var status = (TaskStatus)reader.ReadInt32();
            var createTime = reader.ReadInstantNullable();
            var startTime = reader.ReadInstantNullable();
            var endTime = reader.ReadInstantNullable();

            return new TaskState(id, status, createTime.GetValueOrDefault(), startTime, endTime);
        }

        private IJobExecution<T> GetJobExecution<T>(
            PooledBuffer computeExecuteResult,
            bool readSchema,
            Func<Memory<byte>?, T>? marshaller)
        {
            var reader = computeExecuteResult.GetReader();

            if (readSchema)
            {
                _ = reader.ReadInt32();
            }

            var jobId = reader.ReadGuid();
            var resultTask = GetResult((NotificationHandler)computeExecuteResult.Metadata!);

            return new JobExecution<T>(jobId, resultTask, this);

            static async Task<(T, JobState)> GetResult(NotificationHandler handler)
            {
                using var notificationRes = await handler.Task.ConfigureAwait(false);
                return Read(notificationRes.GetReader());
            }

            static (T, JobState) Read(MsgPackReader reader)
            {
                var res = (T)reader.ReadObjectFromBinaryTuple()!;
                var status = ReadJobState(reader);

                return (res, status);
            }
        }

        private ITaskExecution<T> GetTaskExecution<T>(PooledBuffer computeExecuteResult)
        {
            var reader = computeExecuteResult.GetReader();

            var taskId = reader.ReadGuid();

            var jobCount = reader.ReadInt32();
            List<Guid> jobIds = new(jobCount);

            for (var i = 0; i < jobCount; i++)
            {
                jobIds.Add(reader.ReadGuid());
            }

            var resultTask = GetResult((NotificationHandler)computeExecuteResult.Metadata!);

            return new TaskExecution<T>(taskId, jobIds, resultTask, this);

            static async Task<(T, TaskState)> GetResult(NotificationHandler handler)
            {
                using var notificationRes = await handler.Task.ConfigureAwait(false);
                return Read(notificationRes.GetReader());
            }

            static (T, TaskState) Read(MsgPackReader reader)
            {
                var res = (T)reader.ReadObjectFromBinaryTuple()!;
                var state = ReadTaskState(reader);

                return (res, state);
            }
        }

        private async Task<IJobExecution<TResult>> ExecuteOnNodes<TArg, TResult>(
            ICollection<IClusterNode> nodes,
            JobDescriptor<TArg, TResult> jobDescriptor,
            TArg arg)
        {
            IClusterNode node = GetRandomNode(nodes);
            JobExecutionOptions options = jobDescriptor.Options ?? JobExecutionOptions.Default;

            using var writer = ProtoCommon.GetMessageWriter();
            Write();

            using PooledBuffer res = await _socket.DoOutInOpAsync(
                    ClientOp.ComputeExecute, writer, PreferredNode.FromName(node.Name), expectNotifications: true)
                .ConfigureAwait(false);

            return GetJobExecution<TResult>(res, readSchema: false, jobDescriptor.ResultMarshaller);

            void Write()
            {
                var w = writer.MessageWriter;

                WriteNodeNames(nodes, writer);
                WriteUnits(GetUnitsCollection(jobDescriptor.DeploymentUnits), writer);
                w.Write(jobDescriptor.JobClassName);
                w.Write(options.Priority);
                w.Write(options.MaxRetries);

                w.WriteObjectAsBinaryTuple(arg, jobDescriptor.ArgMarshaller);
            }
        }

        private async Task<Table> GetTableAsync(string tableName)
        {
            if (_tableCache.TryGetValue(tableName, out var cachedTable))
            {
                return cachedTable;
            }

            var table = await _tables.GetTableInternalAsync(tableName).ConfigureAwait(false);

            if (table != null)
            {
                _tableCache[tableName] = table;
                return table;
            }

            _tableCache.TryRemove(tableName, out _);

            throw new IgniteClientException(ErrorGroups.Client.TableIdNotFound, $"Table '{tableName}' does not exist.");
        }

        [SuppressMessage("Maintainability", "CA1508:Avoid dead conditional code", Justification = "False positive")]
        private async Task<IJobExecution<TResult>> ExecuteColocatedAsync<TArg, TResult, TKey>(
            string tableName,
            TKey key,
            Func<Table, IRecordSerializerHandler<TKey>> serializerHandlerFunc,
            JobDescriptor<TArg, TResult> descriptor,
            TArg arg)
            where TKey : notnull
        {
            var options = descriptor.Options ?? JobExecutionOptions.Default;
            var units0 = GetUnitsCollection(descriptor.DeploymentUnits);

            int? schemaVersion = null;

            while (true)
            {
                var table = await GetTableAsync(tableName).ConfigureAwait(false);
                var schema = await table.GetSchemaAsync(schemaVersion).ConfigureAwait(false);

                try
                {
                    using var bufferWriter = ProtoCommon.GetMessageWriter();
                    var colocationHash = Write(bufferWriter, table, schema);
                    var preferredNode = await table.GetPreferredNode(colocationHash, null).ConfigureAwait(false);

                    using var res = await _socket.DoOutInOpAsync(
                            ClientOp.ComputeExecuteColocated, bufferWriter, preferredNode, expectNotifications: true)
                        .ConfigureAwait(false);

                    return GetJobExecution<TResult>(res, readSchema: true);
                }
                catch (IgniteException e) when (e.Code == ErrorGroups.Client.TableIdNotFound)
                {
                    // Table was dropped - remove from cache.
                    // Try again in case a new table with the same name exists.
                    _tableCache.TryRemove(tableName, out _);
                    schemaVersion = null;
                }
                catch (IgniteException e) when (e.Code == ErrorGroups.Table.SchemaVersionMismatch &&
                                                schemaVersion != e.GetExpectedSchemaVersion())
                {
                    schemaVersion = e.GetExpectedSchemaVersion();
                }
                catch (Exception e) when (e.CausedByUnmappedColumns() &&
                                          schemaVersion == null)
                {
                    schemaVersion = Table.SchemaVersionForceLatest;
                }
            }

            int Write(PooledArrayBuffer bufferWriter, Table table, Schema schema)
            {
                var w = bufferWriter.MessageWriter;

                w.Write(table.Id);
                w.Write(schema.Version);

                var serializerHandler = serializerHandlerFunc(table);
                var colocationHash = serializerHandler.Write(ref w, schema, key, keyOnly: true, computeHash: true);

                WriteUnits(units0, bufferWriter);
                w.Write(descriptor.JobClassName);
                w.Write(options.Priority);
                w.Write(options.MaxRetries);

                w.WriteObjectAsBinaryTuple(arg);

                return colocationHash;
            }
        }

        private async Task<IJobExecution<TResult>> SubmitAsync<TArg, TResult>(
            IEnumerable<IClusterNode> nodes,
            JobDescriptor<TArg, TResult> jobDescriptor,
            TArg arg)
        {
            IgniteArgumentCheck.NotNull(nodes);
            IgniteArgumentCheck.NotNull(jobDescriptor);
            IgniteArgumentCheck.NotNull(jobDescriptor.JobClassName);

            ICollection<IClusterNode> nodesCol = GetNodesCollection(nodes);
            IgniteArgumentCheck.Ensure(nodesCol.Count > 0, nameof(nodes), "Nodes can't be empty.");

            return await ExecuteOnNodes(nodesCol, jobDescriptor, arg).ConfigureAwait(false);
        }

        private async Task<IJobExecution<TResult>> SubmitColocatedAsync<TArg, TResult, TKey>(
            JobTarget.ColocatedTarget<TKey> target,
            JobDescriptor<TArg, TResult> jobDescriptor,
            TArg arg)
            where TKey : notnull
        {
            IgniteArgumentCheck.NotNull(jobDescriptor);

            if (target.Data is IIgniteTuple tuple)
            {
                return await ExecuteColocatedAsync<TArg, TResult, IIgniteTuple>(
                        target.TableName,
                        tuple,
                        static _ => TupleSerializerHandler.Instance,
                        jobDescriptor,
                        arg)
                    .ConfigureAwait(false);
            }

            return await ExecuteColocatedAsync<TArg, TResult, TKey>(
                    target.TableName,
                    target.Data,
                    static table => table.GetRecordViewInternal<TKey>().RecordSerializer.Handler,
                    jobDescriptor,
                    arg)
                .ConfigureAwait(false);
        }
    }
}
