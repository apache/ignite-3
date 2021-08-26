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
    using System.Buffers;
    using System.Diagnostics.CodeAnalysis;
    using System.Net;
    using System.Net.Sockets;
    using System.Threading.Tasks;
    using Buffers;
    using MessagePack;
    using Proto;

    /// <summary>
    /// Wrapper over framework socket for Ignite thin client operations.
    ///
    /// TODO:
    /// * Assembly signing.
    /// </summary>
    // ReSharper disable SuggestBaseTypeForParameter (NetworkStream has more efficient read/write methods).
    internal sealed class ClientSocket : IDisposable
    {
        /** General-purpose client type code. */
        private const byte ClientType = 2;

        /** Version 3.0.0. */
        private static readonly ClientProtocolVersion Ver300 = new(3, 0, 0);

        /** Current version. */
        private static readonly ClientProtocolVersion CurrentProtocolVersion = Ver300;

        /** Underlying stream. */
        private readonly NetworkStream _stream;

        /// <summary>
        /// Initializes a new instance of the <see cref="ClientSocket"/> class.
        /// </summary>
        /// <param name="stream">Network stream.</param>
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
                await WriteHandshakeAsync(stream, CurrentProtocolVersion).ConfigureAwait(false);

                await stream.FlushAsync().ConfigureAwait(false);

                await CheckMagicBytesAsync(stream).ConfigureAwait(false);
                await CheckHandshakeResponseAsync(stream).ConfigureAwait(false);

                return new ClientSocket(stream);
            }
            catch (Exception)
            {
                // ReSharper disable once MethodHasAsyncOverload
                stream.Dispose();

                throw;
            }
        }

        /// <inheritdoc/>
        public void Dispose()
        {
            _stream.Dispose();
        }

        private static async Task CheckMagicBytesAsync(NetworkStream stream)
        {
            var responseMagic = ArrayPool<byte>.Shared.Rent(4);

            try
            {
                await stream.ReadAsync(responseMagic).ConfigureAwait(false);

                for (var i = 0; i < responseMagic.Length; i++)
                {
                    if (responseMagic[i] != ProtoCommon.MagicBytes[i])
                    {
                        throw new IgniteClientException("Invalid magic bytes returned from the server: " +
                                                        BitConverter.ToString(responseMagic));
                    }
                }
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(responseMagic);
            }
        }

        private static async Task CheckHandshakeResponseAsync(NetworkStream stream)
        {
            var response = await ReadResponseAsync(stream).ConfigureAwait(false);

            try
            {
                CheckHandshakeResponse(response.GetUnpacker());
            }
            finally
            {
                response.Release();
            }
        }

        private static void CheckHandshakeResponse(MessagePackReader reader)
        {
            var serverVer = new ClientProtocolVersion(reader.ReadInt16(), reader.ReadInt16(), reader.ReadInt16());

            if (serverVer != CurrentProtocolVersion)
            {
                throw new IgniteClientException("Unexpected server version: " + serverVer);
            }

            var errorCode = (ClientErrorCode)reader.ReadInt32();

            if (errorCode != ClientErrorCode.Success)
            {
                var errorMessage = reader.ReadString();

                throw new IgniteClientException(errorMessage, null, errorCode);
            }

            reader.Skip(); // Features.
            reader.Skip(); // Extensions.
        }

        private static async Task<PooledBuf> ReadResponseAsync(NetworkStream stream)
        {
            var size = await ReadMessageSizeAsync(stream).ConfigureAwait(false);

            var bytes = ArrayPool<byte>.Shared.Rent(size);

            await stream.ReadAsync(bytes.AsMemory(0, size)).ConfigureAwait(false);

            return new PooledBuf(bytes, size);
        }

        private static async Task<int> ReadMessageSizeAsync(NetworkStream stream)
        {
            const int messageSizeByteCount = 4;
            var bytes = ArrayPool<byte>.Shared.Rent(messageSizeByteCount);

            try
            {
                await stream.ReadAsync(bytes.AsMemory(0, messageSizeByteCount)).ConfigureAwait(false);

                return GetMessageSize(bytes);
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(bytes);
            }
        }

        private static unsafe int GetMessageSize(byte[] responseLenBytes)
        {
            fixed (int* len = &responseLenBytes[0])
            {
                return *len;
            }
        }

        private static async Task WriteHandshakeAsync(NetworkStream stream, ClientProtocolVersion version)
        {
            // TODO:
            // 1. Avoid allocating PooledArrayBufferWriter - but how?
            //    - struct with interface will box
            // 2. Flush packer automatically: use delegates (allocatey) or don't (more boilerplate)
            using var bufferWriter = new PooledArrayBufferWriter();
            WriteHandshake(version, bufferWriter.GetPacker());

            await stream.WriteAsync(bufferWriter.GetWrittenMemory()).ConfigureAwait(false);
        }

        private static void WriteHandshake(ClientProtocolVersion version, MessagePackWriter w)
        {
            // Version.
            w.Write(version.Major);
            w.Write(version.Minor);
            w.Write(version.Patch);

            w.Write(ClientType); // Client type: general purpose.

            w.WriteBinHeader(0); // Features.
            w.WriteMapHeader(0); // Extensions.

            w.Flush();
        }
    }
}
