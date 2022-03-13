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
    using System.Buffers;
    using System.Net;
    using System.Net.Sockets;
    using System.Threading;
    using System.Threading.Tasks;
    using Internal.Proto;
    using MessagePack;

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

        public FakeServer(Func<int, bool>? shouldDropConnection = null)
        {
            _shouldDropConnection = shouldDropConnection ?? (_ => false);
            _listener = new Socket(IPAddress.Loopback.AddressFamily, SocketType.Stream, ProtocolType.Tcp);

            _listener.Bind(new IPEndPoint(IPAddress.Loopback, 0));
            _listener.Listen(backlog: 1);

            Task.Run(ListenLoop);
        }

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

        private static int ReceiveMessageSize(Socket handler) =>
            IPAddress.NetworkToHostOrder(BitConverter.ToInt32(ReceiveBytes(handler, 4)));

        private static byte[] ReceiveBytes(Socket socket, int size)
        {
            int received = 0;
            var buf = new byte[size];

            while (received < size)
            {
                var res = socket.Receive(buf, received, size - received, SocketFlags.None);

                if (res == 0)
                {
                    throw new Exception("Connection lost");
                }

                received += res;
            }

            return buf;
        }

        private void ListenLoop()
        {
            int requestCount = 0;

            while (!_cts.IsCancellationRequested)
            {
                using Socket handler = _listener.Accept();

                // Read handshake.
                ReceiveBytes(handler, 4); // Magic.
                var msgSize = ReceiveMessageSize(handler);
                ReceiveBytes(handler, msgSize);

                // Write handshake response.
                handler.Send(ProtoCommon.MagicBytes);
                handler.Send(new byte[] { 0, 0, 0, 7 }); // Size.
                handler.Send(new byte[] { 3, 0, 0, 0, 196, 0, 128 });

                while (!_cts.IsCancellationRequested)
                {
                    msgSize = ReceiveMessageSize(handler);
                    var msg = ReceiveBytes(handler, msgSize);

                    if (_shouldDropConnection(++requestCount))
                    {
                        break;
                    }

                    // Assume fixint8.
                    var opCode = (ClientOp)msg[0];
                    var requestId = msg[1];

                    if (opCode == ClientOp.TablesGet)
                    {
                        handler.Send(new byte[] { 0, 0, 0, 4 }); // Size.
                        handler.Send(new byte[] { 0, requestId, 0, 128 }); // Empty map.

                        continue;
                    }

                    if (opCode == ClientOp.TableGet)
                    {
                        var reader = new MessagePackReader(msg.AsMemory()[2..]);
                        var tableName = reader.ReadString();

                        if (tableName == ExistingTableName)
                        {
                            handler.Send(new byte[] { 0, 0, 0, 21 }); // Size.
                            handler.Send(new byte[] { 0, requestId, 0 });

                            var arrayBufferWriter = new ArrayBufferWriter<byte>();
                            var writer = new MessagePackWriter(arrayBufferWriter);
                            writer.Write(Guid.Empty);
                            writer.Flush();

                            handler.Send(arrayBufferWriter.WrittenSpan);

                            continue;
                        }
                    }

                    if (opCode == ClientOp.SchemasGet)
                    {
                        handler.Send(new byte[] { 0, 0, 0, 6 }); // Size.
                        handler.Send(new byte[] { 0, requestId, 0 });

                        var arrayBufferWriter = new ArrayBufferWriter<byte>();
                        var writer = new MessagePackWriter(arrayBufferWriter);
                        writer.WriteMapHeader(1);
                        writer.Write(1); // Version.
                        writer.WriteArrayHeader(0); // Columns.
                        writer.Flush();

                        handler.Send(arrayBufferWriter.WrittenSpan);

                        continue;
                    }

                    if (opCode == ClientOp.TupleUpsert)
                    {
                        handler.Send(new byte[] { 0, 0, 0, 3 }); // Size.
                        handler.Send(new byte[] { 0, requestId, 0 }); // No payload.

                        continue;
                    }

                    if (opCode == ClientOp.TxBegin)
                    {
                        handler.Send(new byte[] { 0, 0, 0, 4 }); // Size.
                        handler.Send(new byte[] { 0, requestId, 0, 0 }); // Tx id.

                        continue;
                    }

                    // Fake error message for any other op code.
                    handler.Send(new byte[] { 0, 0, 0, 8 }); // Size.
                    handler.Send(new byte[] { 0, requestId, 1, 160 | 4, (byte)Err[0], (byte)Err[1], (byte)Err[2], (byte)Err[3] });
                }

                handler.Disconnect(true);
            }
        }
    }
}
