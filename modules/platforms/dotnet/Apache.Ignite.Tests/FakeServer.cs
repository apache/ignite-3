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

namespace Apache.Ignite.Tests
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Linq;
    using System.Net;
    using System.Net.Sockets;
    using System.Threading;
    using System.Threading.Tasks;
    using Ignite.Compute;
    using Ignite.Sql;
    using Internal.Buffers;
    using Internal.Common;
    using Internal.Network;
    using Internal.Proto;
    using Internal.Proto.BinaryTuple;
    using Internal.Proto.MsgPack;
    using MessagePack;
    using Network;

    /// <summary>
    /// Fake Ignite server for test purposes.
    /// </summary>
    public sealed class FakeServer : IgniteServerBase
    {
        public const string Err = "Err!";

        public const string ExistingTableName = "tbl1";

        public const string CompositeKeyTableName = "tbl2";

        public const string CustomColocationKeyTableName = "tbl3";

        public const string GetDetailsJob = "get-details";

        private const int ExistingTableId = 1001;

        private const int CompositeKeyTableId = 1002;

        private const int CustomColocationKeyTableId = 1003;

        private readonly Func<RequestContext, bool> _shouldDropConnection;

        private readonly ConcurrentQueue<ClientOp>? _ops;

        public FakeServer(bool disableOpsTracking)
            : this(null, disableOpsTracking: disableOpsTracking)
        {
            // No-op.
        }

        public FakeServer(bool disableOpsTracking, string nodeName = "fake-server")
            : this(null, nodeName, disableOpsTracking: disableOpsTracking)
        {
            // No-op.
        }

        internal FakeServer(
            Func<RequestContext, bool>? shouldDropConnection = null,
            string nodeName = "fake-server",
            bool disableOpsTracking = false)
        {
            _shouldDropConnection = shouldDropConnection ?? (_ => false);

            Node = new ClusterNode("id-" + nodeName, nodeName, (IPEndPoint)Listener.LocalEndPoint!);
            PartitionAssignment = new[] { Node.Id };

            if (!disableOpsTracking)
            {
                _ops = new();
            }
        }

        public IClusterNode Node { get; }

        public Guid ClusterId { get; set; }

        public string ClusterName { get; set; } = "fake-cluster";

        public string[] PartitionAssignment { get; set; }

        public long PartitionAssignmentTimestamp { get; set; }

        public TimeSpan HandshakeDelay { get; set; }

        public TimeSpan OperationDelay { get; set; }

        public TimeSpan MultiRowOperationDelayPerRow { get; set; }

        public TimeSpan HeartbeatDelay { get; set; }

        public string LastSql { get; set; } = string.Empty;

        public long? LastSqlTimeoutMs { get; set; }

        public int? LastSqlPageSize { get; set; }

        public long? LastSqlTxId { get; set; }

        public Dictionary<string, object?> LastSqlScriptProps { get; private set; } = new();

        public long StreamerRowCount { get; set; }

        public long DroppedConnectionCount { get; set; }

        public bool SendInvalidMagic { get; set; }

        public int RequestCount { get; set; }

        public long ObservableTimestamp { get; set; }

        public long LastClientObservableTimestamp { get; set; }

        internal IList<ClientOp> ClientOps => _ops?.ToList() ?? throw new Exception("Ops tracking is disabled");

        public async Task<IIgniteClient> ConnectClientAsync(IgniteClientConfiguration? cfg = null)
        {
            cfg ??= new IgniteClientConfiguration();

            cfg.Endpoints.Clear();
            cfg.Endpoints.Add(Endpoint);

            return await IgniteClient.StartAsync(cfg);
        }

        public void ClearOps() => _ops?.Clear();

        protected override void Handle(Socket handler, CancellationToken cancellationToken)
        {
            // Read handshake.
            using var magic = ReceiveBytes(handler, 4);
            var msgSize = ReceiveMessageSize(handler);
            using var handshake = ReceiveBytes(handler, msgSize);

            // Write handshake response.
            handler.Send(SendInvalidMagic ? ProtoCommon.MagicBytes.Reverse().ToArray() : ProtoCommon.MagicBytes);
            Thread.Sleep(HandshakeDelay);

            using var handshakeBufferWriter = new PooledArrayBuffer();
            var handshakeWriter = handshakeBufferWriter.MessageWriter;

            // Version.
            handshakeWriter.Write(3);
            handshakeWriter.Write(0);
            handshakeWriter.Write(0);

            handshakeWriter.WriteNil(); // Success
            handshakeWriter.Write(0); // Idle timeout.
            handshakeWriter.Write(Node.Id); // Node id.
            handshakeWriter.Write(Node.Name); // Node name (consistent id).

            handshakeWriter.Write(ClusterId);
            handshakeWriter.Write(ClusterName);

            handshakeWriter.Write(ObservableTimestamp);

            // Cluster version.
            handshakeWriter.Write(1);
            handshakeWriter.Write(2);
            handshakeWriter.Write(3);
            handshakeWriter.Write(4);
            handshakeWriter.Write("-abcd");

            handshakeWriter.WriteBinaryHeader(0); // Features.
            handshakeWriter.Write(0); // Extensions.

            var handshakeMem = handshakeBufferWriter.GetWrittenMemory();
            handler.Send(new byte[] { 0, 0, 0, (byte)handshakeMem.Length }); // Size.

            handler.Send(handshakeMem.Span);

            while (!cancellationToken.IsCancellationRequested)
            {
                msgSize = ReceiveMessageSize(handler);
                using var msg = ReceiveBytes(handler, msgSize);

                if (OperationDelay > TimeSpan.Zero)
                {
                    Thread.Sleep(OperationDelay);
                }

                var reader = msg.GetReader();
                var opCode = (ClientOp)reader.ReadInt32();
                var requestId = reader.ReadInt64();

                if (_shouldDropConnection(new RequestContext(++RequestCount, opCode, requestId)))
                {
                    DroppedConnectionCount++;
                    break;
                }

                _ops?.Enqueue(opCode);

                switch (opCode)
                {
                    case ClientOp.TablesGet:
                        // Zero tables.
                        Send(handler, requestId, new byte[] { 0 }.AsMemory());
                        continue;

                    case ClientOp.TableGet:
                    {
                        var tableName = reader.ReadString();

                        var tableId = tableName switch
                        {
                            ExistingTableName => ExistingTableId,
                            CompositeKeyTableName => CompositeKeyTableId,
                            CustomColocationKeyTableName => CustomColocationKeyTableId,
                            _ => default
                        };

                        if (tableId != default)
                        {
                            using var arrayBufferWriter = new PooledArrayBuffer();
                            arrayBufferWriter.MessageWriter.Write(0); // zone id.
                            arrayBufferWriter.MessageWriter.Write(tableId);

                            Send(handler, requestId, arrayBufferWriter);

                            continue;
                        }

                        break;
                    }

                    case ClientOp.SchemasGet:
                        GetSchemas(reader, handler, requestId);
                        continue;

                    case ClientOp.PartitionAssignmentGet:
                    {
                        using var arrayBufferWriter = new PooledArrayBuffer();
                        var writer = new MsgPackWriter(arrayBufferWriter);
                        writer.Write(PartitionAssignment.Length);
                        writer.Write(true); // Assignment available.
                        writer.Write(DateTime.UtcNow.Ticks); // Timestamp

                        foreach (var nodeId in PartitionAssignment)
                        {
                            writer.Write(nodeId);
                        }

                        Send(handler, requestId, arrayBufferWriter);
                        continue;
                    }

                    case ClientOp.TupleUpsert:
                        Send(handler, requestId, ReadOnlyMemory<byte>.Empty);
                        continue;

                    case ClientOp.TupleInsert:
                    case ClientOp.TupleReplace:
                    case ClientOp.TupleReplaceExact:
                    case ClientOp.TupleDelete:
                    case ClientOp.TupleDeleteExact:
                    case ClientOp.TupleContainsKey:
                        Send(handler, requestId, new byte[] { 1, MessagePackCode.True }.AsMemory());
                        continue;

                    case ClientOp.TupleGet:
                    case ClientOp.TupleGetAndDelete:
                    case ClientOp.TupleGetAndReplace:
                    case ClientOp.TupleGetAndUpsert:
                        Send(handler, requestId, new byte[] { 1, MessagePackCode.Nil }.AsMemory());
                        continue;

                    case ClientOp.TupleGetAll:
                    case ClientOp.TupleInsertAll:
                    case ClientOp.TupleUpsertAll:
                    case ClientOp.TupleDeleteAll:
                    case ClientOp.TupleDeleteAllExact:
                        reader.Skip(3);
                        var count = reader.ReadInt32();

                        if (MultiRowOperationDelayPerRow > TimeSpan.Zero)
                        {
                            Thread.Sleep(MultiRowOperationDelayPerRow * count);
                        }

                        Send(handler, requestId, new byte[] { 1, 0 }.AsMemory());
                        continue;

                    case ClientOp.TxBegin:
                        reader.Skip(); // Read only.
                        LastClientObservableTimestamp = reader.ReadInt64();

                        Send(handler, requestId, new byte[] { 0 }.AsMemory());
                        continue;

                    case ClientOp.ComputeExecute:
                    case ClientOp.ComputeExecuteColocated:
                    {
                        using var pooledArrayBuffer = ComputeExecute(reader, colocated: opCode == ClientOp.ComputeExecuteColocated);

                        using var resWriter = new PooledArrayBuffer();

                        var rw = resWriter.MessageWriter;
                        if (opCode == ClientOp.ComputeExecuteColocated)
                        {
                            // Schema version.
                            rw.Write(1);
                        }

                        rw.Write(Guid.NewGuid());

                        Send(handler, requestId, resWriter);
                        Send(handler, requestId, pooledArrayBuffer, isNotification: true);
                        continue;
                    }

                    case ClientOp.SqlExec:
                        SqlExec(handler, requestId, reader);
                        continue;

                    case ClientOp.SqlCursorNextPage:
                        SqlCursorNextPage(handler, requestId);
                        continue;

                    case ClientOp.SqlExecScript:
                        SqlExecScript(reader);
                        Send(handler, requestId, Array.Empty<byte>());
                        continue;

                    case ClientOp.Heartbeat:
                        Thread.Sleep(HeartbeatDelay);
                        Send(handler, requestId, Array.Empty<byte>());
                        continue;

                    case ClientOp.StreamerBatchSend:
                        reader.Skip(4);
                        var batchSize = reader.ReadInt32();
                        StreamerRowCount += batchSize;

                        if (MultiRowOperationDelayPerRow > TimeSpan.Zero)
                        {
                            Thread.Sleep(MultiRowOperationDelayPerRow * batchSize);
                        }

                        Send(handler, requestId, Array.Empty<byte>());
                        continue;
                }

                // Fake error message for any other op code.
                using var errWriter = new PooledArrayBuffer();
                var w = new MsgPackWriter(errWriter);
                w.Write(Guid.Empty);
                w.Write(262150);
                w.Write("org.foo.bar.BazException");
                w.Write(Err);
                w.WriteNil(); // Stack trace.
                w.WriteNil(); // Error extensions.

                Send(handler, requestId, errWriter, isError: true);
            }

            handler.Disconnect(true);
        }

        private void Send(Socket socket, long requestId, PooledArrayBuffer writer, bool isError = false, bool isNotification = false)
            => Send(socket, requestId, writer.GetWrittenMemory(), isError, isNotification);

        private void Send(Socket socket, long requestId, ReadOnlyMemory<byte> payload, bool isError = false, bool isNotification = false)
        {
            using var header = new PooledArrayBuffer();
            var writer = new MsgPackWriter(header);

            writer.Write(requestId);

            var flags = (int)ResponseFlags.PartitionAssignmentChanged;

            if (isError)
            {
                flags |= (int)ResponseFlags.Error;
            }

            if (isNotification)
            {
                flags |= (int)ResponseFlags.Notification;
            }

            writer.Write(flags);
            writer.Write(PartitionAssignmentTimestamp);

            writer.Write(ObservableTimestamp); // Observable timestamp.

            var headerMem = header.GetWrittenMemory();
            var size = BitConverter.GetBytes(IPAddress.HostToNetworkOrder(headerMem.Length + payload.Length));
            socket.Send(size);

            socket.Send(headerMem.Span);

            if (!payload.IsEmpty)
            {
                socket.Send(payload.Span);
            }
        }

        private void SqlCursorNextPage(Socket handler, long requestId)
        {
            using var arrayBufferWriter = new PooledArrayBuffer();
            var writer = new MsgPackWriter(arrayBufferWriter);

            writer.Write(500); // Page size.
            for (int i = 0; i < 500; i++)
            {
                using var tuple = new BinaryTupleBuilder(1);
                tuple.AppendInt(i + 512);
                writer.Write(tuple.Build().Span);
            }

            writer.Write(false); // Has next.

            Send(handler, requestId, arrayBufferWriter);
        }

        private void SqlExec(Socket handler, long requestId, MsgPackReader reader)
        {
            var props = new Dictionary<string, object?>();

            // ReSharper disable RedundantCast (does not build on older SDKs)
            var txId = reader.TryReadNil() ? (long?)null : reader.ReadInt64();
            props["txId"] = txId;
            props["schema"] = reader.TryReadNil() ? null : reader.ReadString();

            var pageSize = reader.TryReadNil() ? (int?)null : reader.ReadInt32();
            props["pageSize"] = pageSize;

            var timeoutMs = reader.TryReadNil() ? (long?)null : reader.ReadInt64();
            props["timeoutMs"] = timeoutMs;

            props["sessionTimeoutMs"] = reader.TryReadNil() ? (long?)null : reader.ReadInt64();
            props["timeZoneId"] = reader.TryReadNil() ? null : reader.ReadString();

            // ReSharper restore RedundantCast
            var propCount = reader.ReadInt32();
            var propTuple = new BinaryTupleReader(reader.ReadBinary(), propCount * 4);

            for (int i = 0; i < propCount; i++)
            {
                var idx = i * 4;

                var name = propTuple.GetString(idx);
                var type = (ColumnType)propTuple.GetInt(idx + 1);
                var scale = propTuple.GetInt(idx + 2);

                props[name] = propTuple.GetObject(idx + 3, type, scale);
            }

            var sql = reader.ReadString();
            props["sql"] = sql;

            if (!reader.TryReadNil())
            {
                var argCount = reader.ReadInt32();
                if (argCount > 0)
                {
                    reader.Skip();
                }
            }

            LastClientObservableTimestamp = reader.ReadInt64();

            LastSql = sql;
            LastSqlPageSize = pageSize;
            LastSqlTimeoutMs = timeoutMs;
            LastSqlTxId = txId;

            using var arrayBufferWriter = new PooledArrayBuffer();
            var writer = new MsgPackWriter(arrayBufferWriter);

            writer.Write(1); // ResourceId.

            if (sql == "SELECT PROPS")
            {
                writer.Write(true); // HasRowSet.
                writer.Write(false); // hasMore.
                writer.Write(false); // WasApplied.
                writer.Write(0); // AffectedRows.

                writer.Write(2); // Meta.

                writer.Write(6); // Column props.
                writer.Write("NAME"); // Column name.
                writer.Write(false); // Nullable.
                writer.Write((int)ColumnType.String);
                writer.Write(0); // Scale.
                writer.Write(0); // Precision.
                writer.Write(false); // No origin.

                writer.Write(6); // Column props.
                writer.Write("VAL"); // Column name.
                writer.Write(false); // Nullable.
                writer.Write((int)ColumnType.String);
                writer.Write(0); // Scale.
                writer.Write(0); // Precision.
                writer.Write(false); // No origin.

                writer.Write(props.Count); // Page size.
                foreach (var (key, val) in props)
                {
                    using var tuple = new BinaryTupleBuilder(2);
                    tuple.AppendString(key);
                    tuple.AppendString(val?.ToString() ?? string.Empty);
                    writer.Write(tuple.Build().Span);
                }
            }
            else
            {
                writer.Write(true); // HasRowSet.
                writer.Write(true); // hasMore.
                writer.Write(false); // WasApplied.
                writer.Write(0); // AffectedRows.

                writer.Write(1); // Meta.
                writer.Write(6); // Column props.
                writer.Write("ID"); // Column name.
                writer.Write(false); // Nullable.
                writer.Write((int)ColumnType.Int32);
                writer.Write(0); // Scale.
                writer.Write(0); // Precision.
                writer.Write(false); // No origin.

                writer.Write(512); // Page size.
                for (int i = 0; i < 512; i++)
                {
                    using var tuple = new BinaryTupleBuilder(1);
                    tuple.AppendInt(i);
                    writer.Write(tuple.Build().Span);
                }
            }

            Send(handler, requestId, arrayBufferWriter);
        }

        private void SqlExecScript(MsgPackReader reader)
        {
            var props = new Dictionary<string, object?>
            {
                ["schema"] = reader.TryReadNil() ? null : reader.ReadString(),
                ["pageSize"] = reader.TryReadNil() ? null : reader.ReadInt32(),
                ["timeoutMs"] = reader.TryReadNil() ? null : reader.ReadInt64(),
                ["sessionTimeoutMs"] = reader.TryReadNil() ? null : reader.ReadInt64(),
                ["timeZoneId"] = reader.TryReadNil() ? null : reader.ReadString()
            };

            var propCount = reader.ReadInt32();
            var propTuple = new BinaryTupleReader(reader.ReadBinary(), propCount * 4);

            for (int i = 0; i < propCount; i++)
            {
                var idx = i * 4;

                var name = propTuple.GetString(idx);
                var type = (ColumnType)propTuple.GetInt(idx + 1);
                var scale = propTuple.GetInt(idx + 2);

                props[name] = propTuple.GetObject(idx + 3, type, scale);
            }

            var sql = reader.ReadString();
            props["sql"] = sql;

            LastSqlScriptProps = props;
        }

        private void GetSchemas(MsgPackReader reader, Socket handler, long requestId)
        {
            var tableId = reader.ReadInt32();

            using var arrayBufferWriter = new PooledArrayBuffer();
            var writer = new MsgPackWriter(arrayBufferWriter);
            writer.Write(1);
            writer.Write(1); // Version.

            if (tableId == ExistingTableId)
            {
                writer.Write(1); // Columns.
                writer.Write(7); // Column props.
                writer.Write("ID");
                writer.Write((int)ColumnType.Int32);
                writer.Write(0); // Key index.
                writer.Write(false); // Nullable.
                writer.Write(0); // Colocation index.
                writer.Write(0); // Scale.
                writer.Write(0); // Precision.
            }
            else if (tableId == CompositeKeyTableId)
            {
                writer.Write(2); // Columns.

                writer.Write(7); // Column props.
                writer.Write("IdStr");
                writer.Write((int)ColumnType.String);
                writer.Write(0); // Key index.
                writer.Write(false); // Nullable.
                writer.Write(0); // Colocation index.
                writer.Write(0); // Scale.
                writer.Write(0); // Precision.

                writer.Write(7); // Column props.
                writer.Write("IdGuid");
                writer.Write((int)ColumnType.Uuid);
                writer.Write(1); // Key index.
                writer.Write(false); // Nullable.
                writer.Write(1); // Colocation index.
                writer.Write(0); // Scale.
                writer.Write(0); // Precision.
            }
            else if (tableId == CustomColocationKeyTableId)
            {
                writer.Write(2); // Columns.

                writer.Write(7); // Column props.
                writer.Write("IdStr");
                writer.Write((int)ColumnType.String);
                writer.Write(0); // Key index.
                writer.Write(false); // Nullable.
                writer.Write(0); // Colocation index.
                writer.Write(0); // Scale.
                writer.Write(0); // Precision.

                writer.Write(7); // Column props.
                writer.Write("IdGuid");
                writer.Write((int)ColumnType.Uuid);
                writer.Write(1); // Key index.
                writer.Write(false); // Nullable.
                writer.Write(-1); // Colocation index.
                writer.Write(0); // Scale.
                writer.Write(0); // Precision.
            }

            Send(handler, requestId, arrayBufferWriter);
        }

        private PooledArrayBuffer ComputeExecute(MsgPackReader reader, bool colocated = false)
        {
            // Colocated: table id, schema version, key.
            // Else: node names.
            if (colocated)
            {
                reader.Skip(4);
            }
            else
            {
                var namesCount = reader.ReadInt32();
                for (int i = 0; i < namesCount; i++)
                {
                    reader.ReadString();
                }
            }

            var unitsCount = reader.TryReadNil() ? 0 : reader.ReadInt32();
            var units = new List<DeploymentUnit>(unitsCount);
            for (int i = 0; i < unitsCount; i++)
            {
                units.Add(new DeploymentUnit(reader.ReadString(), reader.ReadString()));
            }

            var jobClassName = reader.ReadString();
            var priority = reader.ReadInt32();
            var maxRetries = reader.ReadInt64();

            object? resObj = jobClassName == GetDetailsJob
                ? new
                {
                    NodeName = Node.Name,
                    Units = units.Select(u => $"{u.Name}|{u.Version}").StringJoin(),
                    jobClassName,
                    priority,
                    maxRetries
                }.ToString()
                : Node.Name;

            using var builder = new BinaryTupleBuilder(3);
            builder.AppendObjectWithType(resObj);

            var arrayBufferWriter = new PooledArrayBuffer();
            var writer = new MsgPackWriter(arrayBufferWriter);

            writer.Write(builder.Build().Span);

            // Status
            writer.Write(Guid.NewGuid());
            writer.Write(0); // State.
            writer.Write(0L); // Create time.
            writer.Write(0);
            writer.WriteNil(); // Start time.
            writer.WriteNil(); // Finish time.

            return arrayBufferWriter;
        }

        internal record struct RequestContext(int RequestCount, ClientOp OpCode, long RequestId);
    }
}
