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
    using System.Collections.Concurrent;
    using System.Diagnostics;
    using System.Diagnostics.CodeAnalysis;
    using System.Net;
    using System.Net.Sockets;
    using System.Threading;
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

        /** Current async operations, map from request id. */
        private readonly ConcurrentDictionary<long, TaskCompletionSource<PooledBuffer>> _requests = new();

        /** Requests can be sent by one thread at a time.  */
        [SuppressMessage(
            "Microsoft.Design",
            "CA2213:DisposableFieldsShouldBeDisposed",
            Justification = "WaitHandle is not used in SemaphoreSlim, no need to dispose.")]
        private readonly SemaphoreSlim _sendLock = new(initialCount: 1);

        /** Cancellation token source that gets cancelled when this instance is disposed. */
        [SuppressMessage(
            "Microsoft.Design",
            "CA2213:DisposableFieldsShouldBeDisposed",
            Justification = "WaitHandle is not used in CancellationTokenSource, no need to dispose.")]
        private readonly CancellationTokenSource _disposeTokenSource = new();

        /** Request id generator. */
        private long _requestId;

        /// <summary>
        /// Initializes a new instance of the <see cref="ClientSocket"/> class.
        /// </summary>
        /// <param name="stream">Network stream.</param>
        private ClientSocket(NetworkStream stream)
        {
            _stream = stream;

            // Because this call is not awaited, execution of the current method continues before the call is completed.
            // Receive loop runs in the background and should not be awaited.
#pragma warning disable 4014
            RunReceiveLoop(_disposeTokenSource.Token);
#pragma warning restore 4014
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

        /// <summary>
        /// Performs an in-out operation.
        /// </summary>
        /// <param name="request">Request data.</param>
        /// <returns>Response data.</returns>
        public Task<PooledBuffer> DoOutInOpAsync(PooledArrayBufferWriter request)
        {
            Debug.Assert(request.RequestId != null, "request.RequestId != null");
            var requestId = request.RequestId.Value;

            var taskCompletionSource = new TaskCompletionSource<PooledBuffer>();

            _requests[requestId] = taskCompletionSource;

            SendRequestAsync(request)
                .AsTask()
                .ContinueWith(
                    (task, state) =>
                    {
                        if (task.Exception != null)
                        {
                            ((TaskCompletionSource<PooledBuffer>)state!).TrySetException(task.Exception);
                        }
                    },
                    taskCompletionSource,
                    _disposeTokenSource.Token,
                    TaskContinuationOptions.None,
                    TaskScheduler.Default);

            return taskCompletionSource.Task;
        }

        /// <summary>
        /// Gets the request writer for the specified operation.
        /// </summary>
        /// <param name="clientOp">Operation code.</param>
        /// <returns>Request writer.</returns>
        public PooledArrayBufferWriter GetRequestWriter(ClientOp clientOp)
        {
            var requestId = Interlocked.Increment(ref _requestId);
            var bufferWriter = new PooledArrayBufferWriter(requestId);

            var writer = bufferWriter.GetMessageWriter();

            writer.Write((int)clientOp);
            writer.Write(requestId);

            writer.Flush();

            return bufferWriter;
        }

        /// <inheritdoc/>
        public void Dispose()
        {
            _disposeTokenSource.Cancel();
            _stream.Dispose();
        }

        private static async ValueTask CheckMagicBytesAsync(NetworkStream stream)
        {
            var responseMagic = ArrayPool<byte>.Shared.Rent(ProtoCommon.MagicBytes.Length);

            try
            {
                await stream.ReadAsync(responseMagic.AsMemory(0, ProtoCommon.MagicBytes.Length)).ConfigureAwait(false);

                for (var i = 0; i < ProtoCommon.MagicBytes.Length; i++)
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

        private static async ValueTask CheckHandshakeResponseAsync(NetworkStream stream)
        {
            var response = await ReadResponseAsync(stream, CancellationToken.None).ConfigureAwait(false);

            try
            {
                CheckHandshakeResponse(response.GetReader());
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

        private static async ValueTask<PooledBuffer> ReadResponseAsync(
            NetworkStream stream,
            CancellationToken cancellationToken)
        {
            var size = await ReadMessageSizeAsync(stream, cancellationToken).ConfigureAwait(false);

            size = IPAddress.NetworkToHostOrder(size);

            var bytes = ArrayPool<byte>.Shared.Rent(size);

            await stream.ReadAsync(bytes.AsMemory(0, size), cancellationToken).ConfigureAwait(false);

            return new PooledBuffer(bytes, size);
        }

        private static async Task<int> ReadMessageSizeAsync(NetworkStream stream, CancellationToken cancellationToken)
        {
            const int messageSizeByteCount = 4;
            var bytes = ArrayPool<byte>.Shared.Rent(messageSizeByteCount);

            try
            {
                await stream.ReadAsync(bytes.AsMemory(0, messageSizeByteCount), cancellationToken).ConfigureAwait(false);

                return GetMessageSize(bytes);
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(bytes);
            }
        }

        private static unsafe int GetMessageSize(byte[] responseLenBytes)
        {
            fixed (byte* len = &responseLenBytes[0])
            {
                return *(int*)len;
            }
        }

        private static async ValueTask WriteHandshakeAsync(NetworkStream stream, ClientProtocolVersion version)
        {
            // TODO:
            // 1. Avoid allocating PooledArrayBufferWriter - but how?
            //    - struct with interface will box
            // 2. Flush packer automatically: use delegates (allocatey) or don't (more boilerplate)
            using var bufferWriter = new PooledArrayBufferWriter();
            WriteHandshake(version, bufferWriter.GetMessageWriter());

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

        private async ValueTask SendRequestAsync(PooledArrayBufferWriter request)
        {
            await _sendLock.WaitAsync(_disposeTokenSource.Token).ConfigureAwait(false);

            try
            {
                await _stream.WriteAsync(request.GetWrittenMemory(), _disposeTokenSource.Token).ConfigureAwait(false);
            }
            finally
            {
                _sendLock.Release();
            }
        }

        private async Task RunReceiveLoop(CancellationToken cancellationToken)
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                // TODO: Handle responses
                // TODO: Move continuations to thread pool to avoid starving .NET SocketAsyncEngine.EventLoop?
                var response = await ReadResponseAsync(_stream, cancellationToken).ConfigureAwait(false);
            }
        }
    }
}
