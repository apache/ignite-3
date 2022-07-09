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
    using Ignite.Sql;
    using Internal.Buffers;
    using Internal.Network;
    using Internal.Proto;
    using MessagePack;
    using Network;

    /// <summary>
    /// Fake Ignite server for test purposes.
    /// </summary>
    public sealed class FakeServer : IDisposable
    {
        public const string Err = "Err!";

        public const string ExistingTableName = "tbl1";

        private readonly Socket _listener;

        private readonly CancellationTokenSource _cts = new();

        private readonly Func<int, bool> _shouldDropConnection;

        private readonly ConcurrentQueue<ClientOp> _ops = new();

        public FakeServer(Func<int, bool>? shouldDropConnection = null, string nodeName = "fake-server")
        {
            _shouldDropConnection = shouldDropConnection ?? (_ => false);
            _listener = new Socket(IPAddress.Loopback.AddressFamily, SocketType.Stream, ProtocolType.Tcp);

            _listener.Bind(new IPEndPoint(IPAddress.Loopback, 0));
            _listener.Listen(backlog: 1);

            Node = new ClusterNode("id-" + nodeName, nodeName, (IPEndPoint)_listener.LocalEndPoint);

            Task.Run(ListenLoop);
        }

        public IClusterNode Node { get; }

        internal IList<ClientOp> ClientOps => _ops.ToList();

        public async Task<IIgniteClient> ConnectClientAsync(IgniteClientConfiguration? cfg = null)
        {
            var port = ((IPEndPoint)_listener.LocalEndPoint).Port;

            cfg ??= new IgniteClientConfiguration();

            cfg.Endpoints.Clear();
            cfg.Endpoints.Add("127.0.0.1:" + port);

            return await IgniteClient.StartAsync(cfg);
        }

        public void Dispose()
        {
            _cts.Cancel();
            _listener.Disconnect(true);
            _listener.Dispose();
            _cts.Dispose();
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
                    throw new Exception("Connection lost");
                }

                received += res;
            }

            return new PooledBuffer(buf, 0, size);
        }

        private static void Send(Socket socket, long requestId, PooledArrayBufferWriter writer, int resultCode = 0)
            => Send(socket, requestId, writer.GetWrittenMemory().Slice(PooledArrayBufferWriter.ReservedPrefixSize), resultCode);

        private static void Send(Socket socket, long requestId, ReadOnlyMemory<byte> payload, int resultCode = 0)
        {
            var header = new PooledArrayBufferWriter();
            var writer = new MessagePackWriter(header);

            writer.Write(0); // Message type.
            writer.Write(requestId);
            writer.Write(resultCode); // Success.

            writer.Flush();

            var headerMem = header.GetWrittenMemory().Slice(PooledArrayBufferWriter.ReservedPrefixSize);
            var size = BitConverter.GetBytes(IPAddress.HostToNetworkOrder(headerMem.Length + payload.Length));
            socket.Send(size);

            socket.Send(headerMem.Span);

            if (!payload.IsEmpty)
            {
                socket.Send(payload.Span);
            }
        }

        private void ListenLoop()
        {
            int requestCount = 0;

            while (!_cts.IsCancellationRequested)
            {
                using Socket handler = _listener.Accept();

                // Read handshake.
                using var magic = ReceiveBytes(handler, 4);
                var msgSize = ReceiveMessageSize(handler);
                using var handshake = ReceiveBytes(handler, msgSize);

                // Write handshake response.
                handler.Send(ProtoCommon.MagicBytes);

                using var handshakeBufferWriter = new PooledArrayBufferWriter();
                var handshakeWriter = handshakeBufferWriter.GetMessageWriter();
                handshakeWriter.Write(0); // Idle timeout.
                handshakeWriter.Write(Node.Id); // Node id.
                handshakeWriter.Write(Node.Name); // Node name (consistent id).
                handshakeWriter.WriteBinHeader(0); // Features.
                handshakeWriter.WriteMapHeader(0); // Extensions.
                handshakeWriter.Flush();

                var handshakeMem = handshakeBufferWriter.GetWrittenMemory().Slice(PooledArrayBufferWriter.ReservedPrefixSize);
                handler.Send(new byte[] { 0, 0, 0, (byte)(4 + handshakeMem.Length) }); // Size.
                handler.Send(new byte[] { 3, 0, 0, 0 }); // Version and success flag.

                handler.Send(handshakeMem.Span);

                while (!_cts.IsCancellationRequested)
                {
                    msgSize = ReceiveMessageSize(handler);
                    using var msg = ReceiveBytes(handler, msgSize);

                    if (_shouldDropConnection(++requestCount))
                    {
                        break;
                    }

                    var reader = new MessagePackReader(msg.AsMemory());
                    var opCode = (ClientOp)reader.ReadInt32();
                    var requestId = reader.ReadInt64();

                    _ops.Enqueue(opCode);

                    if (opCode == ClientOp.TablesGet)
                    {
                        // Empty map.
                        Send(handler, requestId, new byte[] { 128 }.AsMemory());

                        continue;
                    }

                    if (opCode == ClientOp.TableGet)
                    {
                        var tableName = reader.ReadString();

                        if (tableName == ExistingTableName)
                        {
                            var arrayBufferWriter = new PooledArrayBufferWriter();
                            var writer = new MessagePackWriter(arrayBufferWriter);
                            writer.Write(Guid.Empty);
                            writer.Flush();

                            Send(handler, requestId, arrayBufferWriter);

                            continue;
                        }
                    }

                    if (opCode == ClientOp.SchemasGet)
                    {
                        var arrayBufferWriter = new PooledArrayBufferWriter();
                        var writer = new MessagePackWriter(arrayBufferWriter);
                        writer.WriteMapHeader(1);
                        writer.Write(1); // Version.
                        writer.WriteArrayHeader(0); // Columns.
                        writer.Flush();

                        Send(handler, requestId, arrayBufferWriter);

                        continue;
                    }

                    if (opCode == ClientOp.TupleUpsert)
                    {
                        Send(handler, requestId, ReadOnlyMemory<byte>.Empty);

                        continue;
                    }

                    if (opCode == ClientOp.TxBegin)
                    {
                        Send(handler, requestId, new byte[] { 0 }.AsMemory());

                        continue;
                    }

                    if (opCode == ClientOp.ComputeExecute)
                    {
                        var arrayBufferWriter = new PooledArrayBufferWriter();
                        var writer = new MessagePackWriter(arrayBufferWriter);
                        writer.WriteObjectWithType(Node.Name);
                        writer.Flush();

                        Send(handler, requestId, arrayBufferWriter);

                        continue;
                    }

                    if (opCode == ClientOp.SqlExec)
                    {
                        var arrayBufferWriter = new PooledArrayBufferWriter();
                        var writer = new MessagePackWriter(arrayBufferWriter);

                        writer.Write(1); // ResourceId.
                        writer.Write(true); // HasRowSet.
                        writer.Write(true); // hasMore.
                        writer.Write(false); // WasApplied.
                        writer.Write(0); // AffectedRows.

                        writer.WriteArrayHeader(1); // Meta.
                        writer.Write("ID"); // Column name.
                        writer.Write(false); // Nullable.
                        writer.Write((int)SqlColumnType.Int32);
                        writer.Write(0); // Scale.
                        writer.Write(0); // Precision.
                        writer.Write(false); // No origin.

                        writer.WriteArrayHeader(512); // Page size.
                        for (int i = 0; i < 512; i++)
                        {
                            writer.Write(i); // Row of one.
                        }

                        writer.Flush();

                        Send(handler, requestId, arrayBufferWriter);

                        continue;
                    }

                    if (opCode == ClientOp.SqlCursorNextPage)
                    {
                        var arrayBufferWriter = new PooledArrayBufferWriter();
                        var writer = new MessagePackWriter(arrayBufferWriter);

                        writer.WriteArrayHeader(500); // Page size.
                        for (int i = 0; i < 500; i++)
                        {
                            writer.Write(i); // Row of one.
                        }

                        writer.Write(false); // Has next.
                        writer.Flush();

                        Send(handler, requestId, arrayBufferWriter);

                        continue;
                    }

                    // Fake error message for any other op code.
                    var errWriter = new PooledArrayBufferWriter();
                    var w = new MessagePackWriter(errWriter);
                    w.Write(Err);
                    w.Flush();
                    Send(handler, requestId, errWriter, 1);
                }

                handler.Disconnect(true);
            }
        }
    }
}
