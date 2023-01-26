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
    using System.Diagnostics.CodeAnalysis;
    using System.Linq;
    using System.Net;
    using System.Net.Sockets;
    using System.Threading;
    using System.Threading.Tasks;
    using Ignite.Sql;
    using Internal.Buffers;
    using Internal.Network;
    using Internal.Proto;
    using Internal.Proto.BinaryTuple;
    using Internal.Proto.MsgPack;
    using MessagePack;
    using Network;

    /// <summary>
    /// Fake Ignite server for test purposes.
    /// </summary>
    public sealed class FakeServer : IDisposable
    {
        public const string Err = "Err!";

        public const string ExistingTableName = "tbl1";

        public const string CompositeKeyTableName = "tbl2";

        public const string CustomColocationKeyTableName = "tbl3";

        private static readonly Guid ExistingTableId = Guid.NewGuid();

        private static readonly Guid CompositeKeyTableId = Guid.NewGuid();

        private static readonly Guid CustomColocationKeyTableId = Guid.NewGuid();

        private readonly Socket _listener;

        private readonly CancellationTokenSource _cts = new();

        private readonly Func<int, bool> _shouldDropConnection;

        private readonly ConcurrentQueue<ClientOp>? _ops;

        private readonly object _disposeSyncRoot = new();

        private bool _disposed;

        private Socket? _handler;

        public FakeServer(
            Func<int, bool>? shouldDropConnection = null,
            string nodeName = "fake-server",
            bool disableOpsTracking = false)
        {
            _shouldDropConnection = shouldDropConnection ?? (_ => false);
            _listener = new Socket(IPAddress.Loopback.AddressFamily, SocketType.Stream, ProtocolType.Tcp);
            _listener.NoDelay = true;

            _listener.Bind(new IPEndPoint(IPAddress.Loopback, 0));
            _listener.Listen(backlog: 1);

            Node = new ClusterNode("id-" + nodeName, nodeName, (IPEndPoint)_listener.LocalEndPoint!);
            PartitionAssignment = new[] { Node.Id };

            if (!disableOpsTracking)
            {
                _ops = new();
            }

            Task.Run(ListenLoop);
        }

        public IClusterNode Node { get; }

        public Guid ClusterId { get; set; }

        public string[] PartitionAssignment { get; set; }

        public bool PartitionAssignmentChanged { get; set; }

        public TimeSpan HandshakeDelay { get; set; }

        public TimeSpan HeartbeatDelay { get; set; }

        public int Port => ((IPEndPoint)_listener.LocalEndPoint!).Port;

        public string Endpoint => "127.0.0.1:" + Port;

        public string LastSql { get; set; } = string.Empty;

        public long? LastSqlTimeoutMs { get; set; }

        public int? LastSqlPageSize { get; set; }

        public long? LastSqlTxId { get; set; }

        internal IList<ClientOp> ClientOps => _ops?.ToList() ?? throw new Exception("Ops tracking is disabled");

        public async Task<IIgniteClient> ConnectClientAsync(IgniteClientConfiguration? cfg = null)
        {
            cfg ??= new IgniteClientConfiguration();

            cfg.Endpoints.Clear();
            cfg.Endpoints.Add(Endpoint);

            return await IgniteClient.StartAsync(cfg);
        }

        public void ClearOps() => _ops?.Clear();

        public void Dispose()
        {
            lock (_disposeSyncRoot)
            {
                if (_disposed)
                {
                    return;
                }

                _cts.Cancel();
                _handler?.Dispose();
                _listener.Disconnect(false);
                _listener.Dispose();
                _cts.Dispose();

                _disposed = true;
            }
        }

        private static int ReceiveMessageSize(Socket handler)
        {
            using var buf = ReceiveBytes(handler, 4);
            return IPAddress.NetworkToHostOrder(BitConverter.ToInt32(buf.AsMemory().Span));
        }

        private static PooledBuffer ReceiveBytes(Socket socket, int size)
        {
            int received = 0;
            var buf = ByteArrayPool.Rent(size);

            while (received < size)
            {
                var res = socket.Receive(buf, received, size - received, SocketFlags.None);

                if (res == 0)
                {
                    throw new ConnectionLostException();
                }

                received += res;
            }

            return new PooledBuffer(buf, 0, size);
        }

        private void Send(Socket socket, long requestId, PooledArrayBuffer writer, bool isError = false)
            => Send(socket, requestId, writer.GetWrittenMemory(), isError);

        private void Send(Socket socket, long requestId, ReadOnlyMemory<byte> payload, bool isError = false)
        {
            using var header = new PooledArrayBuffer();
            var writer = new MsgPackWriter(header);

            writer.Write(0); // Message type.
            writer.Write(requestId);
            writer.Write(PartitionAssignmentChanged ? (int)ResponseFlags.PartitionAssignmentChanged : 0);

            if (!isError)
            {
                writer.WriteNil(); // Success.
            }

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

            writer.WriteArrayHeader(500); // Page size.
            for (int i = 0; i < 500; i++)
            {
                using var tuple = new BinaryTupleBuilder(1, false);
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

            // ReSharper restore RedundantCast
            var propCount = reader.ReadInt32();
            var propTuple = new BinaryTupleReader(reader.ReadBinary(), propCount * 4);

            for (int i = 0; i < propCount; i++)
            {
                var idx = i * 4;

                var name = propTuple.GetString(idx);
                var type = (ClientDataType)propTuple.GetInt(idx + 1);
                var scale = propTuple.GetInt(idx + 2);

                props[name] = propTuple.GetObject(idx + 3, type, scale);
            }

            var sql = reader.ReadString();
            props["sql"] = sql;

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

                writer.WriteArrayHeader(2); // Meta.

                writer.WriteArrayHeader(6); // Column props.
                writer.Write("NAME"); // Column name.
                writer.Write(false); // Nullable.
                writer.Write((int)SqlColumnType.String);
                writer.Write(0); // Scale.
                writer.Write(0); // Precision.
                writer.Write(false); // No origin.

                writer.WriteArrayHeader(6); // Column props.
                writer.Write("VAL"); // Column name.
                writer.Write(false); // Nullable.
                writer.Write((int)SqlColumnType.String);
                writer.Write(0); // Scale.
                writer.Write(0); // Precision.
                writer.Write(false); // No origin.

                writer.WriteArrayHeader(props.Count); // Page size.
                foreach (var (key, val) in props)
                {
                    using var tuple = new BinaryTupleBuilder(2, false);
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

                writer.WriteArrayHeader(1); // Meta.
                writer.WriteArrayHeader(6); // Column props.
                writer.Write("ID"); // Column name.
                writer.Write(false); // Nullable.
                writer.Write((int)SqlColumnType.Int32);
                writer.Write(0); // Scale.
                writer.Write(0); // Precision.
                writer.Write(false); // No origin.

                writer.WriteArrayHeader(512); // Page size.
                for (int i = 0; i < 512; i++)
                {
                    using var tuple = new BinaryTupleBuilder(1, false);
                    tuple.AppendInt(i);
                    writer.Write(tuple.Build().Span);
                }
            }

            Send(handler, requestId, arrayBufferWriter);
        }

        private void GetSchemas(MsgPackReader reader, Socket handler, long requestId)
        {
            var tableId = reader.ReadGuid();

            using var arrayBufferWriter = new PooledArrayBuffer();
            var writer = new MsgPackWriter(arrayBufferWriter);
            writer.WriteMapHeader(1);
            writer.Write(1); // Version.

            if (tableId == ExistingTableId)
            {
                writer.WriteArrayHeader(1); // Columns.
                writer.WriteArrayHeader(6); // Column props.
                writer.Write("ID");
                writer.Write((int)ClientDataType.Int32);
                writer.Write(true); // Key.
                writer.Write(false); // Nullable.
                writer.Write(true); // Colocation.
                writer.Write(0); // Scale.
            }
            else if (tableId == CompositeKeyTableId)
            {
                writer.WriteArrayHeader(2); // Columns.

                writer.WriteArrayHeader(6); // Column props.
                writer.Write("IdStr");
                writer.Write((int)ClientDataType.String);
                writer.Write(true); // Key.
                writer.Write(false); // Nullable.
                writer.Write(true); // Colocation.
                writer.Write(0); // Scale.

                writer.WriteArrayHeader(6); // Column props.
                writer.Write("IdGuid");
                writer.Write((int)ClientDataType.Uuid);
                writer.Write(true); // Key.
                writer.Write(false); // Nullable.
                writer.Write(true); // Colocation.
                writer.Write(0); // Scale.
            }
            else if (tableId == CustomColocationKeyTableId)
            {
                writer.WriteArrayHeader(2); // Columns.

                writer.WriteArrayHeader(6); // Column props.
                writer.Write("IdStr");
                writer.Write((int)ClientDataType.String);
                writer.Write(true); // Key.
                writer.Write(false); // Nullable.
                writer.Write(true); // Colocation.
                writer.Write(0); // Scale.

                writer.WriteArrayHeader(6); // Column props.
                writer.Write("IdGuid");
                writer.Write((int)ClientDataType.Uuid);
                writer.Write(true); // Key.
                writer.Write(false); // Nullable.
                writer.Write(false); // Colocation.
                writer.Write(0); // Scale.
            }

            Send(handler, requestId, arrayBufferWriter);
        }

        private void ListenLoop()
        {
            while (!_cts.IsCancellationRequested)
            {
                try
                {
                    ListenLoopInternal();
                }
                catch (Exception e)
                {
                    if (e is SocketException or ConnectionLostException)
                    {
                        continue;
                    }

                    Console.WriteLine("Error in FakeServer: " + e);
                }
            }
        }

        private void ListenLoopInternal()
        {
            int requestCount = 0;

            while (!_cts.IsCancellationRequested)
            {
                using Socket handler = _listener.Accept();
                _handler = handler;

                handler.NoDelay = true;

                // Read handshake.
                using var magic = ReceiveBytes(handler, 4);
                var msgSize = ReceiveMessageSize(handler);
                using var handshake = ReceiveBytes(handler, msgSize);

                // Write handshake response.
                handler.Send(ProtoCommon.MagicBytes);
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
                handshakeWriter.WriteBinaryHeader(0); // Features.
                handshakeWriter.WriteMapHeader(0); // Extensions.

                var handshakeMem = handshakeBufferWriter.GetWrittenMemory();
                handler.Send(new byte[] { 0, 0, 0, (byte)handshakeMem.Length }); // Size.

                handler.Send(handshakeMem.Span);

                while (!_cts.IsCancellationRequested)
                {
                    msgSize = ReceiveMessageSize(handler);
                    using var msg = ReceiveBytes(handler, msgSize);

                    if (_shouldDropConnection(++requestCount))
                    {
                        break;
                    }

                    var reader = new MsgPackReader(msg.AsMemory().Span);
                    var opCode = (ClientOp)reader.ReadInt32();
                    var requestId = reader.ReadInt64();

                    _ops?.Enqueue(opCode);

                    switch (opCode)
                    {
                        case ClientOp.TablesGet:
                            // Empty map.
                            Send(handler, requestId, new byte[] { 128 }.AsMemory());
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
                            writer.WriteArrayHeader(PartitionAssignment.Length);

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
                            Send(handler, requestId, new[] { MessagePackCode.True }.AsMemory());
                            continue;

                        case ClientOp.TupleGet:
                        case ClientOp.TupleGetAll:
                        case ClientOp.TupleGetAndDelete:
                        case ClientOp.TupleGetAndReplace:
                        case ClientOp.TupleGetAndUpsert:
                        case ClientOp.TupleInsertAll:
                        case ClientOp.TupleUpsertAll:
                        case ClientOp.TupleDeleteAll:
                        case ClientOp.TupleDeleteAllExact:
                            Send(handler, requestId, new[] { MessagePackCode.Nil }.AsMemory());
                            continue;

                        case ClientOp.TxBegin:
                            Send(handler, requestId, new byte[] { 0 }.AsMemory());
                            continue;

                        case ClientOp.ComputeExecute:
                        {
                            using var arrayBufferWriter = new PooledArrayBuffer();
                            var writer = new MsgPackWriter(arrayBufferWriter);

                            using var builder = new BinaryTupleBuilder(3);
                            builder.AppendObjectWithType(Node.Name);
                            writer.Write(builder.Build().Span);

                            Send(handler, requestId, arrayBufferWriter);
                            continue;
                        }

                        case ClientOp.SqlExec:
                            SqlExec(handler, requestId, reader);
                            continue;

                        case ClientOp.SqlCursorNextPage:
                            SqlCursorNextPage(handler, requestId);
                            continue;

                        case ClientOp.Heartbeat:
                            Thread.Sleep(HeartbeatDelay);
                            Send(handler, requestId, Array.Empty<byte>());
                            continue;

                        case ClientOp.ComputeExecuteColocated:
                            Send(handler, requestId, new[] { MessagePackCode.Nil }.AsMemory());
                            continue;
                    }

                    // Fake error message for any other op code.
                    using var errWriter = new PooledArrayBuffer();
                    var w = new MsgPackWriter(errWriter);
                    w.Write(Guid.Empty);
                    w.Write(262147);
                    w.Write("org.foo.bar.BazException");
                    w.Write(Err);
                    w.WriteNil(); // Stack trace.

                    Send(handler, requestId, errWriter, isError: true);
                }

                handler.Disconnect(true);
            }
        }

        [SuppressMessage("Design", "CA1032:Implement standard exception constructors", Justification = "Tests.")]
        [SuppressMessage("Design", "CA1064:Exceptions should be public", Justification = "Tests.")]
        private class ConnectionLostException : Exception
        {
        }
    }
}
