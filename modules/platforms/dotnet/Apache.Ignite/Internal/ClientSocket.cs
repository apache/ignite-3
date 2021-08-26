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

namespace Apache.Ignite.Internal
{
    using System;
    using System.Diagnostics.CodeAnalysis;
    using System.Net;
    using System.Net.Sockets;
    using System.Threading.Tasks;
    using Buffers;
    using MessagePack;
    using Proto;

    /// <summary>
    /// Wrapper over framework socket for Ignite thin client operations.
    /// </summary>
    internal class ClientSocket
    {
        /** Underlying stream. */
        private readonly NetworkStream _stream;

        private ClientSocket(NetworkStream stream)
        {
            _stream = stream;
        }

        /// <summary>
        /// Connects the socket to the specified endpoint and performs handshake.
        /// </summary>
        /// <param name="endPoint">Specific endpoint to connect to.</param>
        /// <returns>A <see cref="Task{TResult}"/> representing the result of the asynchronous operation.</returns>
        [SuppressMessage(
            "Microsoft.Reliability",
            "CA2000:Dispose objects before losing scope",
            Justification = "NetworkStream is returned from this method in the socket.")]
        public static async Task<ClientSocket> ConnectAsync(EndPoint endPoint)
        {
            using Socket socket = new(SocketType.Stream, ProtocolType.Tcp)
            {
                NoDelay = true
            };

            await socket.ConnectAsync(endPoint).ConfigureAwait(false);
            var stream = new NetworkStream(socket, ownsSocket: true);

            try
            {
                await stream.WriteAsync(ProtoCommon.MagicBytes).ConfigureAwait(false);
                await WriteHandshakeAsync(stream).ConfigureAwait(false);

                await stream.FlushAsync().ConfigureAwait(false);

                var responseMagic = new byte[4];
                await stream.ReadAsync(responseMagic).ConfigureAwait(false);

                return new ClientSocket(stream);
            }
            catch (Exception)
            {
                // ReSharper disable once MethodHasAsyncOverload
                stream.Dispose();

                throw;
            }
        }

        // ReSharper disable once SuggestBaseTypeForParameter (NetworkStream has a more efficient overload).
        private static async Task WriteHandshakeAsync(NetworkStream stream)
        {
            using var bufferWriter = WriteHandshake();

            await stream.WriteAsync(bufferWriter.GetArray().AsMemory(0, bufferWriter.WrittenCount))
                    .ConfigureAwait(false);
        }

        private static unsafe PooledArrayBufferWriter WriteHandshake()
        {
            var bufferWriter = new PooledArrayBufferWriter();

            try
            {
                bufferWriter.Advance(4);

                var writer = new MessagePackWriter(bufferWriter);

                // Version.
                writer.Write(3);
                writer.Write(0);
                writer.Write(0);

                writer.Write(2); // Client type: general purpose.

                writer.WriteBinHeader(0); // Features.
                writer.WriteMapHeader(0); // Extensions.

                writer.Flush();

                // Write big-endian message size to the start of the buffer.
                var buffer = bufferWriter.GetArray();

                var msgSize = IPAddress.HostToNetworkOrder(bufferWriter.WrittenCount);
                var sizeBytes = new ReadOnlySpan<byte>(&msgSize, 4);
                sizeBytes.CopyTo(buffer);

                return bufferWriter;
            }
            catch (Exception)
            {
                bufferWriter.Dispose();
                throw;
            }
        }
    }
}
