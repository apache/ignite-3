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
    using System.IO;
    using System.Linq;
    using System.Net;
    using System.Net.Sockets;
    using System.Threading.Tasks;
    using MessagePack;
    using NUnit.Framework;

    /// <summary>
    /// Tests protocol basics with simple socket connection.
    /// </summary>
    public class RawSocketConnectionTests : IgniteTestsBase
    {
        private static readonly byte[] Magic = "IGNI".Select(c => (byte)c).ToArray();

        [Test]
        public async Task TestHandshake()
        {
            using Socket socket = new(SocketType.Stream, ProtocolType.Tcp)
            {
                NoDelay = true
            };

            await socket.ConnectAsync(IPAddress.Loopback, JavaServer.ClientPort);
            await using var stream = new NetworkStream(socket, ownsSocket: true);

            await stream.WriteAsync(Magic);
            WriteHandshake(stream);

            await stream.FlushAsync();

            var responseMagic = new byte[4];
            await stream.ReadAsync(responseMagic);

            CollectionAssert.AreEqual(Magic, responseMagic);
        }

        private static unsafe void WriteHandshake(Stream stream)
        {
            // TODO: Buffer pooling.
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

            // Write big-endian message size.
            var msgSize = IPAddress.HostToNetworkOrder(bufferWriter.WrittenCount);

            stream.Write(new ReadOnlySpan<byte>(&msgSize, 4)); // TODO: Async

            // Write message.
            stream.Write(bufferWriter.GetSpan()); // TODO: Async
        }
    }
}
