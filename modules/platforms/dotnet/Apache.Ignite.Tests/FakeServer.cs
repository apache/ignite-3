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
    using System.Net;
    using System.Net.Sockets;
    using System.Threading;
    using System.Threading.Tasks;
    using Internal.Proto;

    /// <summary>
    /// Fake Ignite server for test purposes.
    /// </summary>
    public sealed class FakeServer : IDisposable
    {
        private readonly Socket _listener;

        private readonly CancellationTokenSource _cts = new();

        public FakeServer()
        {
            _listener = new Socket(IPAddress.Loopback.AddressFamily, SocketType.Stream, ProtocolType.Tcp);

            _listener.Bind(new IPEndPoint(IPAddress.Loopback, 0));
            _listener.Listen(backlog: 1);

            Task.Run(ListenLoop);
        }

        public async Task<IIgniteClient> ConnectClientAsync()
        {
            var port = ((IPEndPoint)_listener.LocalEndPoint).Port;

            return await IgniteClient.StartAsync(new IgniteClientConfiguration("127.0.0.1:" + port));
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

                    // Assume fixint8.
                    var opCode = (ClientOp)msg[0];
                    var requestId = msg[1];

                    if (opCode == ClientOp.TablesGet)
                    {
                        handler.Send(new byte[] { 0, 0, 0, 4 }); // Size.
                        handler.Send(new byte[] { 0, requestId, 0, 128 }); // Empty map.
                    }
                    else
                    {
                        // Fake error message for any other op code.
                        handler.Send(new byte[] { 0, 0, 0, 8 }); // Size.
                        handler.Send(new byte[] { 0, requestId, 1, 160 | 4, (byte)'F', (byte)'A', (byte)'K', (byte)'E', });
                    }
                }
            }
        }
    }
}
