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
        public async Task<IJobExecution<T>> SubmitAsync<T>(
            IEnumerable<IClusterNode> nodes,
            IEnumerable<DeploymentUnit> units,
            string jobClassName,
            JobExecutionOptions options,
            params object?[]? args)
        {
            IgniteArgumentCheck.NotNull(nodes);
            IgniteArgumentCheck.NotNull(jobClassName);

            var nodesCol = GetNodesCollection(nodes);
            IgniteArgumentCheck.Ensure(nodesCol.Count > 0, nameof(nodes), "Nodes can't be empty.");

            return await ExecuteOnNodes<T>(nodesCol, units, jobClassName, options, args).ConfigureAwait(false);
        }

        /// <inheritdoc/>
        public async Task<IJobExecution<T>> SubmitColocatedAsync<T>(
            string tableName,
            IIgniteTuple key,
            IEnumerable<DeploymentUnit> units,
            string jobClassName,
            JobExecutionOptions options,
            params object?[]? args) =>
            await ExecuteColocatedAsync<T, IIgniteTuple>(
                    tableName,
                    key,
                    serializerHandlerFunc: static _ => TupleSerializerHandler.Instance,
                    units,
                    jobClassName,
                    options,
                    args)
                .ConfigureAwait(false);

        /// <inheritdoc/>
        public async Task<IJobExecution<T>> SubmitColocatedAsync<T, TKey>(
            string tableName,
            TKey key,
            IEnumerable<DeploymentUnit> units,
            string jobClassName,
            JobExecutionOptions options,
            params object?[]? args)
            where TKey : notnull =>
            await ExecuteColocatedAsync<T, TKey>(
                    tableName,
                    key,
                    serializerHandlerFunc: table => table.GetRecordViewInternal<TKey>().RecordSerializer.Handler,
                    units,
                    jobClassName,
                    options,
                    args)
                .ConfigureAwait(false);

        /// <inheritdoc/>
        public IDictionary<IClusterNode, Task<IJobExecution<T>>> SubmitBroadcast<T>(
            IEnumerable<IClusterNode> nodes,
            IEnumerable<DeploymentUnit> units,
            string jobClassName,
            JobExecutionOptions options,
            params object?[]? args)
        {
            IgniteArgumentCheck.NotNull(nodes);
            IgniteArgumentCheck.NotNull(jobClassName);
            IgniteArgumentCheck.NotNull(units);

            var res = new Dictionary<IClusterNode, Task<IJobExecution<T>>>();
            var units0 = units as ICollection<DeploymentUnit> ?? units.ToList(); // Avoid multiple enumeration.

            foreach (var node in nodes)
            {
                Task<IJobExecution<T>> task = ExecuteOnNodes<T>(new[] { node }, units0, jobClassName, options, args);

                res[node] = task;
            }

            return res;
        }

        /// <inheritdoc/>
        public override string ToString() => IgniteToStringBuilder.Build(GetType());

        /// <summary>
        /// Gets the job status.
        /// </summary>
        /// <param name="jobId">Job ID.</param>
        /// <returns>Status.</returns>
        internal async Task<JobStatus?> GetJobStatusAsync(Guid jobId)
        {
            using var writer = ProtoCommon.GetMessageWriter();
            writer.MessageWriter.Write(jobId);

            using var res = await _socket.DoOutInOpAsync(ClientOp.ComputeGetStatus, writer).ConfigureAwait(false);
            return Read(res.GetReader());

            JobStatus? Read(MsgPackReader reader) => reader.TryReadNil() ? null : ReadJobStatus(reader);
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

        private static void WriteUnits(IEnumerable<DeploymentUnit> units, PooledArrayBuffer buf)
        {
            WriteEnumerable(units, buf, writerFunc: unit =>
            {
                IgniteArgumentCheck.NotNullOrEmpty(unit.Name);
                IgniteArgumentCheck.NotNullOrEmpty(unit.Version);

                var w = buf.MessageWriter;
                w.Write(unit.Name);
                w.Write(unit.Version);
            });
        }

        private static void WriteNodeNames(IEnumerable<IClusterNode> nodes, PooledArrayBuffer buf)
        {
            WriteEnumerable(nodes, buf, writerFunc: node =>
            {
                var w = buf.MessageWriter;
                w.Write(node.Name);
            });
        }

        private static JobStatus ReadJobStatus(MsgPackReader reader)
        {
            var id = reader.ReadGuid();
            var state = (JobState)reader.ReadInt32();
            var createTime = reader.ReadInstantNullable();
            var startTime = reader.ReadInstantNullable();
            var endTime = reader.ReadInstantNullable();

            return new JobStatus(id, state, createTime.GetValueOrDefault(), startTime, endTime);
        }

        private IJobExecution<T> GetJobExecution<T>(PooledBuffer computeExecuteResult, bool readSchema)
        {
            var reader = computeExecuteResult.GetReader();

            if (readSchema)
            {
                _ = reader.ReadInt32();
            }

            var jobId = reader.ReadGuid();
            var resultTask = GetResult((NotificationHandler)computeExecuteResult.Metadata!);

            return new JobExecution<T>(jobId, resultTask, this);

            static async Task<(T, JobStatus)> GetResult(NotificationHandler handler)
            {
                using var notificationRes = await handler.Task.ConfigureAwait(false);
                return Read(notificationRes.GetReader());
            }

            static (T, JobStatus) Read(MsgPackReader reader)
            {
                var res = (T)reader.ReadObjectFromBinaryTuple()!;
                var status = ReadJobStatus(reader);

                return (res, status);
            }
        }

        private async Task<IJobExecution<T>> ExecuteOnNodes<T>(
            ICollection<IClusterNode> nodes,
            IEnumerable<DeploymentUnit> units,
            string jobClassName,
            JobExecutionOptions options,
            object?[]? args)
        {
            IClusterNode node = GetRandomNode(nodes);

            using var writer = ProtoCommon.GetMessageWriter();
            Write();

            using PooledBuffer res = await _socket.DoOutInOpAsync(
                    ClientOp.ComputeExecute, writer, PreferredNode.FromName(node.Name), expectNotifications: true)
                .ConfigureAwait(false);

            return GetJobExecution<T>(res, readSchema: false);

            void Write()
            {
                var w = writer.MessageWriter;

                WriteNodeNames(nodes, writer);
                WriteUnits(units, writer);
                w.Write(jobClassName);
                w.Write(options.Priority);
                w.Write(options.MaxRetries);

                w.WriteObjectCollectionAsBinaryTuple(args);
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
        private async Task<IJobExecution<T>> ExecuteColocatedAsync<T, TKey>(
            string tableName,
            TKey key,
            Func<Table, IRecordSerializerHandler<TKey>> serializerHandlerFunc,
            IEnumerable<DeploymentUnit> units,
            string jobClassName,
            JobExecutionOptions options,
            params object?[]? args)
            where TKey : notnull
        {
            IgniteArgumentCheck.NotNull(tableName);
            IgniteArgumentCheck.NotNull(key);
            IgniteArgumentCheck.NotNull(jobClassName);

            var units0 = units as ICollection<DeploymentUnit> ?? units.ToList(); // Avoid multiple enumeration.
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

                    return GetJobExecution<T>(res, readSchema: true);
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
                w.Write(jobClassName);
                w.Write(options.Priority);
                w.Write(options.MaxRetries);

                w.WriteObjectCollectionAsBinaryTuple(args);

                return colocationHash;
            }
        }
    }
}
