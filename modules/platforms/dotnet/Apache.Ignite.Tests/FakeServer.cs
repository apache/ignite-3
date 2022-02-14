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
            var buf = new byte[1024];

            var listener = new Socket(IPAddress.Loopback.AddressFamily, SocketType.Stream, ProtocolType.Tcp);

            listener.Bind(new IPEndPoint(IPAddress.Loopback, Port));
            listener.Listen(backlog: 1);

            _listenTask = Task.Run(() =>
            {
                while (!_cts.IsCancellationRequested)
                {
                    Socket handler = listener.Accept();

                    while (_cts.IsCancellationRequested)
                    {
                        var bytesRec = handler.Receive(buf);

                        if (bytesRec == -1)
                        {
                            break;
                        }
                    }

                    // TODO: Send response.
                    handler.Send(new byte[1]);

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
    }
}
