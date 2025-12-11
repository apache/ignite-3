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
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Diagnostics.CodeAnalysis;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Buffers;
    using Common;
    using Ignite.Compute;
    using Ignite.Network;
    using Ignite.Table;
    using Marshalling;
    using Network;
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
        private readonly ConcurrentDictionary<QualifiedName, Table> _tableCache = new();

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
            TArg arg,
            CancellationToken cancellationToken)
            where TTarget : notnull
        {
            IgniteArgumentCheck.NotNull(target);
            IgniteArgumentCheck.NotNull(jobDescriptor);

            return target switch
            {
                JobTarget.SingleNodeTarget singleNode => SubmitAsync([singleNode.Data], jobDescriptor, arg, cancellationToken),
                JobTarget.AnyNodeTarget anyNode => SubmitAsync(anyNode.Data, jobDescriptor, arg, cancellationToken),
                JobTarget.ColocatedTarget<TTarget> colocated => SubmitColocatedAsync(colocated, jobDescriptor, arg, cancellationToken),

                _ => throw new ArgumentException("Unsupported job target: " + target)
            };
        }

        /// <inheritdoc/>
        public async Task<IBroadcastExecution<TResult>> SubmitBroadcastAsync<TTarget, TArg, TResult>(
            IBroadcastJobTarget<TTarget> target,
            JobDescriptor<TArg, TResult> jobDescriptor,
            TArg arg,
            CancellationToken cancellationToken)
            where TTarget : notnull
        {
            IgniteArgumentCheck.NotNull(target);
            IgniteArgumentCheck.NotNull(jobDescriptor);
            IgniteArgumentCheck.NotNull(jobDescriptor.JobClassName);

            return target switch
            {
                BroadcastJobTarget.AllNodesTarget allNodes => await SubmitBroadcastAsyncInternal(allNodes.Data)
                    .ConfigureAwait(false),

                _ => throw new ArgumentException("Unsupported broadcast job target: " + target)
            };

            async Task<IBroadcastExecution<TResult>> SubmitBroadcastAsyncInternal(IEnumerable<IClusterNode> nodes)
            {
                var jobExecutions = new List<IJobExecution<TResult>>();

                foreach (var node in nodes)
                {
                    IJobExecution<TResult> jobExec = await ExecuteOnNodes([node], jobDescriptor, arg, cancellationToken)
                        .ConfigureAwait(false);

                    jobExecutions.Add(jobExec);
                }

                return new BroadcastExecution<TResult>(jobExecutions);
            }
        }

        /// <inheritdoc/>
        public async Task<ITaskExecution<TResult>> SubmitMapReduceAsync<TArg, TResult>(
            TaskDescriptor<TArg, TResult> taskDescriptor,
            TArg arg,
            CancellationToken cancellationToken)
        {
            IgniteArgumentCheck.NotNull(taskDescriptor);
            IgniteArgumentCheck.NotNull(taskDescriptor.TaskClassName);

            using var writer = ProtoCommon.GetMessageWriter();
            Write();

            using PooledBuffer res = await _socket.DoOutInOpAsync(
                    ClientOp.ComputeExecuteMapReduce, writer, expectNotifications: true, cancellationToken: cancellationToken)
                .ConfigureAwait(false);

            return GetTaskExecution<TResult>(res, cancellationToken);

            void Write()
            {
                var w = writer.MessageWriter;

                WriteUnits(taskDescriptor.DeploymentUnits, writer);
                w.Write(taskDescriptor.TaskClassName);

                ComputePacker.PackArgOrResult(ref w, arg, null);
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

            WriteEnumerable(units, buf, writerFunc: static (unit, buf) =>
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
        /// Changes the job priority. After priority change, the job will be the last in the queue of jobs with the same priority.
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

        private static void WriteEnumerable<T>(IEnumerable<T> items, PooledArrayBuffer buf, Action<T, PooledArrayBuffer> writerFunc)
        {
            var count = 0;
            var countPos = buf.ReserveMsgPackInt32();

            foreach (var item in items)
            {
                count++;
                writerFunc(item, buf);
            }

            buf.WriteMsgPackInt32(count, countPos);
        }

        private static void WriteNodeNames(PooledArrayBuffer buf, IEnumerable<IClusterNode> nodes) =>
            WriteEnumerable(nodes, buf, writerFunc: static (node, buf) => buf.MessageWriter.Write(node.Name));

        private static void WriteJob<TArg, TResult>(
            PooledArrayBuffer writer,
            JobDescriptor<TArg, TResult> jobDescriptor,
            bool canWriteJobExecType)
        {
            WriteUnits(jobDescriptor.DeploymentUnits, writer);

            var w = writer.MessageWriter;
            w.Write(jobDescriptor.JobClassName);

            var options = jobDescriptor.Options ?? JobExecutionOptions.Default;
            w.Write(options.Priority);
            w.Write(options.MaxRetries);

            if (canWriteJobExecType)
            {
                w.Write((int)options.ExecutorType);
            }
            else if (options.ExecutorType != JobExecutionOptions.Default.ExecutorType)
            {
                throw new IgniteClientException(
                    ErrorGroups.Client.ProtocolCompatibility,
                    $"Job executor type '{options.ExecutorType}' is not supported by the server.");
            }
        }

        private static bool CanWriteJobExecType(ClientSocket socket) =>
            socket.ConnectionContext.ServerHasFeature(ProtocolBitmaskFeature.PlatformComputeJob);

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

        private static void ConvertExceptionAndThrow(IgniteException e, CancellationToken token)
        {
            switch (e.Code)
            {
                case ErrorGroups.Compute.ComputeJobCancelled:
                    var cancelledToken = token.IsCancellationRequested ? token : CancellationToken.None;

                    throw new OperationCanceledException(e.Message, e, cancelledToken);
            }
        }

        private JobExecution<T> GetJobExecution<T>(
            PooledBuffer computeExecuteResult,
            bool readSchema,
            IMarshaller<T>? marshaller,
            CancellationToken cancellationToken)
        {
            var reader = computeExecuteResult.GetReader();

            if (readSchema)
            {
                _ = reader.ReadInt32();
            }

            var jobId = reader.ReadGuid();

            // Standard cancellation by requestId in ClientSocket does not work for jobs.
            // Compute job can be cancelled from any node, it is not bound to a connection.
            var cancellationTokenRegistration = cancellationToken.Register(() => _ = CancelJobAsync(jobId));

            try
            {
                var resultTask = GetResult((NotificationHandler)computeExecuteResult.Metadata!);
                var node = ClusterNode.Read(ref reader);

                return new JobExecution<T>(jobId, resultTask, this, node);
            }
            catch (Exception)
            {
                cancellationTokenRegistration.Dispose();
                throw;
            }

            async Task<(T, JobState)> GetResult(NotificationHandler handler)
            {
                try
                {
                    using var notificationRes = await handler.Task.ConfigureAwait(false);
                    return Read(notificationRes.GetReader());
                }
                catch (IgniteException e)
                {
                    ConvertExceptionAndThrow(e, cancellationToken);
                    throw;
                }
                finally
                {
                    // ReSharper disable once AccessToDisposedClosure
                    await cancellationTokenRegistration.DisposeAsync().ConfigureAwait(false);
                }
            }

            (T, JobState) Read(MsgPackReader reader)
            {
                var res = ComputePacker.UnpackArgOrResult(ref reader, marshaller);
                var status = ReadJobState(reader);

                return (res, status);
            }
        }

        private TaskExecution<T> GetTaskExecution<T>(PooledBuffer computeExecuteResult, CancellationToken cancellationToken)
        {
            var reader = computeExecuteResult.GetReader();

            var taskId = reader.ReadGuid();

            // Standard cancellation by requestId in ClientSocket does not work for jobs.
            // Compute job can be cancelled from any node, it is not bound to a connection.
            var cancellationTokenRegistration = cancellationToken.Register(() => _ = CancelJobAsync(taskId));

            try
            {
                var jobCount = reader.ReadInt32();
                List<Guid> jobIds = new(jobCount);

                for (var i = 0; i < jobCount; i++)
                {
                    jobIds.Add(reader.ReadGuid());
                }

                var resultTask = GetResult((NotificationHandler)computeExecuteResult.Metadata!);

                return new TaskExecution<T>(taskId, jobIds, resultTask, this);
            }
            catch (Exception)
            {
                cancellationTokenRegistration.Dispose();
                throw;
            }

            async Task<(T, TaskState)> GetResult(NotificationHandler handler)
            {
                try
                {
                    using var notificationRes = await handler.Task.ConfigureAwait(false);
                    return Read(notificationRes.GetReader());
                }
                catch (IgniteException e)
                {
                    ConvertExceptionAndThrow(e, cancellationToken);
                    throw;
                }
                finally
                {
                    // ReSharper disable once AccessToDisposedClosure
                    await cancellationTokenRegistration.DisposeAsync().ConfigureAwait(false);
                }
            }

            static (T, TaskState) Read(MsgPackReader reader)
            {
                // TODO IGNITE-23074 .NET: Thin 3.0: Support marshallers in MapReduce
                var res = ComputePacker.UnpackArgOrResult<T>(ref reader, null);
                var state = ReadTaskState(reader);

                return (res, state);
            }
        }

        private async Task<IJobExecution<TResult>> ExecuteOnNodes<TArg, TResult>(
            ICollection<IClusterNode> nodes,
            JobDescriptor<TArg, TResult> jobDescriptor,
            TArg arg,
            CancellationToken cancellationToken)
        {
            IClusterNode node = GetRandomNode(nodes);

            using var buf = await _socket.DoWithRetryAsync(
                    (nodes, jobDescriptor, arg, cancellationToken),
                    static (_, _) => ClientOp.ComputeExecute,
                    async static (socket, args) =>
                    {
                        using var writer = ProtoCommon.GetMessageWriter();
                        Write(writer, args, CanWriteJobExecType(socket));

                        return await socket.DoOutInOpAsync(
                            ClientOp.ComputeExecute, writer, expectNotifications: true, cancellationToken: args.cancellationToken)
                            .ConfigureAwait(false);
                    },
                    PreferredNode.FromName(node.Name))
                .ConfigureAwait(false);

            return GetJobExecution(buf, readSchema: false, jobDescriptor.ResultMarshaller, cancellationToken);

            static void Write(
                PooledArrayBuffer writer,
                (ICollection<IClusterNode> Nodes, JobDescriptor<TArg, TResult> Desc, TArg Arg, CancellationToken Ct) args,
                bool canWriteJobExecType)
            {
                WriteNodeNames(writer, args.Nodes);
                WriteJob(writer, args.Desc, canWriteJobExecType);

                var w = writer.MessageWriter;
                ComputePacker.PackArgOrResult(ref w, args.Arg, args.Desc.ArgMarshaller);
            }
        }

        private async Task<Table> GetTableAsync(QualifiedName tableName)
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

            throw new IgniteClientException(ErrorGroups.Client.TableIdNotFound, $"Table '{tableName.CanonicalName}' does not exist.");
        }

        [SuppressMessage("Maintainability", "CA1508:Avoid dead conditional code", Justification = "False positive")]
        private async Task<IJobExecution<TResult>> ExecuteColocatedAsync<TArg, TResult, TKey>(
            QualifiedName tableName,
            TKey key,
            Func<Table, IRecordSerializerHandler<TKey>> serializerHandlerFunc,
            JobDescriptor<TArg, TResult> descriptor,
            TArg arg,
            CancellationToken cancellationToken)
            where TKey : notnull
        {
            int? schemaVersion = null;

            while (true)
            {
                var table = await GetTableAsync(tableName).ConfigureAwait(false);
                var schema = await table.GetSchemaAsync(schemaVersion).ConfigureAwait(false);

                try
                {
                    // Write the job executor type optimistically, compute hash.
                    using var bufferWriter = ProtoCommon.GetMessageWriter();
                    var colocationHash = Write(bufferWriter, table, schema, key, serializerHandlerFunc, descriptor, arg, true);
                    var preferredNode = await table.GetPreferredNode(colocationHash, null).ConfigureAwait(false);

                    using var resBuf = await _socket.DoWithRetryAsync(
                            (table, schema, key, serializerHandlerFunc, descriptor, arg, bufferWriter, cancellationToken),
                            static (_, _) => ClientOp.ComputeExecuteColocated,
                            async static (socket, args) =>
                            {
                                if (CanWriteJobExecType(socket))
                                {
                                    return await socket.DoOutInOpAsync(ClientOp.ComputeExecuteColocated, args.bufferWriter, expectNotifications: true, cancellationToken: args.cancellationToken)
                                        .ConfigureAwait(false);
                                }

                                // Rewrite the message without a job executor type.
                                using var writer = ProtoCommon.GetMessageWriter();
                                Write(writer, args.table, args.schema, args.key, args.serializerHandlerFunc, args.descriptor, args.arg, false);

                                return await socket.DoOutInOpAsync(ClientOp.ComputeExecuteColocated, writer, expectNotifications: true, cancellationToken: args.cancellationToken)
                                    .ConfigureAwait(false);
                            },
                            preferredNode)
                        .ConfigureAwait(false);

                    return GetJobExecution(resBuf, readSchema: true, marshaller: descriptor.ResultMarshaller, cancellationToken);
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

            static int Write(
                PooledArrayBuffer bufferWriter,
                Table table,
                Schema schema,
                TKey key,
                Func<Table, IRecordSerializerHandler<TKey>> serializerHandlerFunc,
                JobDescriptor<TArg, TResult> descriptor,
                TArg arg,
                bool canWriteJobExecType)
            {
                var w = bufferWriter.MessageWriter;

                w.Write(table.Id);
                w.Write(schema.Version);

                var serializerHandler = serializerHandlerFunc(table);
                var colocationHash = serializerHandler.Write(ref w, schema, key, keyOnly: true, computeHash: true);

                WriteJob(bufferWriter, descriptor, canWriteJobExecType);

                w.WriteObjectAsBinaryTuple(arg);

                return colocationHash;
            }
        }

        private async Task<IJobExecution<TResult>> SubmitAsync<TArg, TResult>(
            IEnumerable<IClusterNode> nodes,
            JobDescriptor<TArg, TResult> jobDescriptor,
            TArg arg,
            CancellationToken cancellationToken)
        {
            IgniteArgumentCheck.NotNull(nodes);
            IgniteArgumentCheck.NotNull(jobDescriptor);
            IgniteArgumentCheck.NotNull(jobDescriptor.JobClassName);

            ICollection<IClusterNode> nodesCol = GetNodesCollection(nodes);
            IgniteArgumentCheck.Ensure(nodesCol.Count > 0, nameof(nodes), "Nodes can't be empty.");

            return await ExecuteOnNodes(nodesCol, jobDescriptor, arg, cancellationToken).ConfigureAwait(false);
        }

        // TODO IGNITE-27278: Remove suppression and require mapper in trimmed mode.
        // Otherwise colocated execution fails for custom types with AOT.
        [UnconditionalSuppressMessage("Trimming", "IL2026", Justification = "All column types are known.")]
        private async Task<IJobExecution<TResult>> SubmitColocatedAsync<TArg, TResult, TKey>(
            JobTarget.ColocatedTarget<TKey> target,
            JobDescriptor<TArg, TResult> jobDescriptor,
            TArg arg,
            CancellationToken cancellationToken)
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
                        arg,
                        cancellationToken)
                    .ConfigureAwait(false);
            }

            return await ExecuteColocatedAsync<TArg, TResult, TKey>(
                    target.TableName,
                    target.Data,
                    static table => table.GetRecordViewInternal<TKey>().RecordSerializer.Handler,
                    jobDescriptor,
                    arg,
                    cancellationToken)
                .ConfigureAwait(false);
        }

        private async Task<bool?> CancelJobAsync(Guid jobId)
        {
            using var writer = ProtoCommon.GetMessageWriter();
            writer.MessageWriter.Write(jobId);

            using var res = await _socket.DoOutInOpAsync(ClientOp.ComputeCancel, writer).ConfigureAwait(false);
            return res.GetReader().ReadBooleanNullable();
        }
    }
}
