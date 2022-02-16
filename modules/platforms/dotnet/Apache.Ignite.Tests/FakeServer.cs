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
        public const int Port = 11222;

        private readonly Task _listenTask;

        private readonly CancellationTokenSource _cts = new();

        public FakeServer()
        {
            var listener = new Socket(IPAddress.Loopback.AddressFamily, SocketType.Stream, ProtocolType.Tcp);

            listener.Bind(new IPEndPoint(IPAddress.Loopback, Port));
            listener.Listen(backlog: 1);

            _listenTask = Task.Run(() =>
            {
                while (!_cts.IsCancellationRequested)
                {
                    Socket handler = listener.Accept();

                    // Read handshake.
                    ReceiveBytes(handler, 4); // Magic.
                    var msgSize = BitConverter.ToInt32(ReceiveBytes(handler, 4));
                    ReceiveBytes(handler, msgSize);

                    // Write handshake response.
                    handler.Send(ProtoCommon.MagicBytes);
                    handler.Send(new byte[] { 7, 0, 0, 0 }); // Size.
                    handler.Send(new byte[] { 3, 0, 0, 0, 196, 0, 128 });

                    while (_cts.IsCancellationRequested)
                    {
                        msgSize = BitConverter.ToInt32(ReceiveBytes(handler, 4));
                        var msg = ReceiveBytes(handler, msgSize);

                        // Assume fixint8.
                        // var opCode = msg[0];
                        var requestId = msg[1];

                        handler.Send(new byte[] { 4, 0, 0, 0 }); // Size.
                        handler.Send(new byte[] { 0, requestId, 1, 192 }); // Error with null message.
                    }

                    handler.Shutdown(SocketShutdown.Both);
                    handler.Close();
                }
            });
        }

        public void Dispose()
        {
            _cts.Cancel();
            _listenTask.Wait();
            _listenTask.Dispose();
            _cts.Dispose();
        }

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
    }
}
