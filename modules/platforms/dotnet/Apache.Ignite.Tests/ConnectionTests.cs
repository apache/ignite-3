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
    using System.Threading.Tasks;
    using MessagePack;
    using NUnit.Framework;

    public class ConnectionTests
    {
        [Test]
        public async Task TestHandshake()
        {
            // TODO: Requires started node - automate this.
            Socket socket = new Socket(SocketType.Stream, ProtocolType.Tcp);
            socket.NoDelay = true;

            try
            {
                await socket.ConnectAsync(IPAddress.Loopback, 10800);
                var stream = new NetworkStream(socket, ownsSocket: true);

                WriteMagic(stream);
                WriteHandshake(stream);

                await stream.FlushAsync();

                var response = new byte[6];
                await stream.ReadAsync(response);
            }
            catch
            {
                socket.Dispose();
                throw;
            }
        }

        private static void WriteMagic(NetworkStream stream)
        {
            // TODO: Async, in one call.
            stream.WriteByte((byte)'I');
            stream.WriteByte((byte)'G');
            stream.WriteByte((byte)'N');
            stream.WriteByte((byte)'I');
        }

        private static unsafe void WriteHandshake(NetworkStream stream)
        {
            var bufferWriter = new ArrayBufferWriter<byte>();
            var writer = new MessagePackWriter(bufferWriter);

            // Version.
            writer.Write(3);
            writer.Write(0);
            writer.Write(0);

            writer.Write(2); // Client type: general purpose.

            writer.WriteBinHeader(0); // Features.
            writer.WriteMapHeader(0); // Extensions.

            writer.Flush();

            // Write little-endian message size (TODO: check endianness).
            var msgSize = (uint)bufferWriter.WrittenCount;
            stream.Write(new ReadOnlySpan<byte>(&msgSize, 4)); // TODO: Async
        }
    }
}
