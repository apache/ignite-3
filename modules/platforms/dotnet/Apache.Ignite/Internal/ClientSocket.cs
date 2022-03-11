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
    using System.Linq;
    using System.Net;
    using System.Net.Sockets;
    using System.Threading;
    using System.Threading.Tasks;
    using Buffers;
    using Log;
    using MessagePack;
    using Proto;

    /// <summary>
    /// Wrapper over framework socket for Ignite thin client operations.
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

        /** Logger. */
        private readonly IIgniteLogger? _logger;

        /** Request id generator. */
        private long _requestId;

        /// <summary>
        /// Initializes a new instance of the <see cref="ClientSocket"/> class.
        /// </summary>
        /// <param name="stream">Network stream.</param>
        /// <param name="logger">Logger.</param>
        private ClientSocket(NetworkStream stream, IIgniteLogger? logger)
        {
            _stream = stream;
            _logger = logger;

            // Because this call is not awaited, execution of the current method continues before the call is completed.
            // Receive loop runs in the background and should not be awaited.
#pragma warning disable 4014
            RunReceiveLoop(_disposeTokenSource.Token);
#pragma warning restore 4014
        }

        /// <summary>
        /// Gets a value indicating whether this socket is disposed.
        /// </summary>
        public bool IsDisposed => _disposeTokenSource.IsCancellationRequested;

        /// <summary>
        /// Connects the socket to the specified endpoint and performs handshake.
        /// </summary>
        /// <param name="endPoint">Specific endpoint to connect to.</param>
        /// <param name="logger">Logger.</param>
        /// <returns>A <see cref="Task{TResult}"/> representing the result of the asynchronous operation.</returns>
        [SuppressMessage(
            "Microsoft.Reliability",
            "CA2000:Dispose objects before losing scope",
            Justification = "NetworkStream is returned from this method in the socket.")]
        public static async Task<ClientSocket> ConnectAsync(EndPoint endPoint, IIgniteLogger? logger = null)
        {
            var socket = new Socket(SocketType.Stream, ProtocolType.Tcp)
            {
                NoDelay = true
            };

            try
            {
                await socket.ConnectAsync(endPoint).ConfigureAwait(false);
                logger?.Debug("Socket connection established: {0} -> {1}", socket.LocalEndPoint, socket.RemoteEndPoint);

                var stream = new NetworkStream(socket, ownsSocket: true);

                await HandshakeAsync(stream).ConfigureAwait(false);

                return new ClientSocket(stream, logger);
            }
            catch (Exception)
            {
                // ReSharper disable once MethodHasAsyncOverload
                socket.Dispose();

                throw;
            }
        }

        /// <summary>
        /// Performs an in-out operation.
        /// </summary>
        /// <param name="clientOp">Client op code.</param>
        /// <param name="request">Request data.</param>
        /// <returns>Response data.</returns>
        public Task<PooledBuffer> DoOutInOpAsync(ClientOp clientOp, PooledArrayBufferWriter? request = null)
        {
            if (_disposeTokenSource.IsCancellationRequested)
            {
                throw new ObjectDisposedException(nameof(ClientSocket));
            }

            var requestId = Interlocked.Increment(ref _requestId);

            var taskCompletionSource = new TaskCompletionSource<PooledBuffer>();

            _requests[requestId] = taskCompletionSource;

            SendRequestAsync(request, clientOp, requestId)
                .AsTask()
                .ContinueWith(
                    (task, state) =>
                    {
                        var completionSource = (TaskCompletionSource<PooledBuffer>)state!;

                        if (task.Exception != null)
                        {
                            completionSource.TrySetException(task.Exception);
                        }
                        else if (task.IsCanceled)
                        {
                            completionSource.TrySetCanceled();
                        }
                    },
                    taskCompletionSource,
                    CancellationToken.None,
                    TaskContinuationOptions.NotOnRanToCompletion,
                    TaskScheduler.Default);

            return taskCompletionSource.Task;
        }

        /// <inheritdoc/>
        public void Dispose()
        {
            Dispose(new ObjectDisposedException("Connection closed."));
        }

        /// <summary>
        /// Performs the handshake exchange.
        /// </summary>
        /// <param name="stream">Network stream.</param>
        private static async Task HandshakeAsync(NetworkStream stream)
        {
            await stream.WriteAsync(ProtoCommon.MagicBytes).ConfigureAwait(false);
            await WriteHandshakeAsync(stream, CurrentProtocolVersion).ConfigureAwait(false);

            await stream.FlushAsync().ConfigureAwait(false);

            await CheckMagicBytesAsync(stream).ConfigureAwait(false);

            using var response = await ReadResponseAsync(stream, new byte[4], CancellationToken.None).ConfigureAwait(false);
            CheckHandshakeResponse(response.GetReader());
        }

        private static async ValueTask CheckMagicBytesAsync(NetworkStream stream)
        {
            var responseMagic = ArrayPool<byte>.Shared.Rent(ProtoCommon.MagicBytes.Length);

            try
            {
                await ReceiveBytesAsync(stream, responseMagic, ProtoCommon.MagicBytes.Length, CancellationToken.None).ConfigureAwait(false);

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

        private static void CheckHandshakeResponse(MessagePackReader reader)
        {
            var serverVer = new ClientProtocolVersion(reader.ReadInt16(), reader.ReadInt16(), reader.ReadInt16());

            if (serverVer != CurrentProtocolVersion)
            {
                throw new IgniteClientException("Unexpected server version: " + serverVer);
            }

            var exception = ReadError(ref reader);

            if (exception != null)
            {
                throw exception;
            }

            reader.Skip(); // Features.
            reader.Skip(); // Extensions.
        }

        private static IgniteClientException? ReadError(ref MessagePackReader reader)
        {
            var errorCode = (ClientErrorCode)reader.ReadInt32();

            if (errorCode != ClientErrorCode.Success)
            {
                var errorMessage = reader.ReadString();

                return new IgniteClientException(errorMessage, null, errorCode);
            }

            return null;
        }

        private static async ValueTask<PooledBuffer> ReadResponseAsync(
            NetworkStream stream,
            byte[] messageSizeBytes,
            CancellationToken cancellationToken)
        {
            var size = await ReadMessageSizeAsync(stream, messageSizeBytes, cancellationToken).ConfigureAwait(false);

            var bytes = ArrayPool<byte>.Shared.Rent(size);

            try
            {
                await ReceiveBytesAsync(stream, bytes, size, cancellationToken).ConfigureAwait(false);

                return new PooledBuffer(bytes, 0, size);
            }
            catch (Exception)
            {
                ArrayPool<byte>.Shared.Return(bytes);

                throw;
            }
        }

        private static async Task<int> ReadMessageSizeAsync(
            NetworkStream stream,
            byte[] buffer,
            CancellationToken cancellationToken)
        {
            const int messageSizeByteCount = 4;
            Debug.Assert(buffer.Length >= messageSizeByteCount, "buffer.Length >= messageSizeByteCount");

            await ReceiveBytesAsync(stream, buffer, messageSizeByteCount, cancellationToken).ConfigureAwait(false);

            return GetMessageSize(buffer);
        }

        private static async Task ReceiveBytesAsync(
            NetworkStream stream,
            byte[] buffer,
            int size,
            CancellationToken cancellationToken)
        {
            int received = 0;

            while (received < size)
            {
                var res = await stream.ReadAsync(buffer.AsMemory(received, size - received), cancellationToken).ConfigureAwait(false);

                if (res == 0)
                {
                    // Disconnected.
                    throw new IgniteClientException(
                        "Connection lost (failed to read data from socket)",
                        new SocketException((int) SocketError.ConnectionAborted));
                }

                received += res;
            }
        }

        private static unsafe int GetMessageSize(byte[] responseLenBytes)
        {
            fixed (byte* len = &responseLenBytes[0])
            {
                var messageSize = *(int*)len;

                return IPAddress.NetworkToHostOrder(messageSize);
            }
        }

        private static async ValueTask WriteHandshakeAsync(NetworkStream stream, ClientProtocolVersion version)
        {
            using var bufferWriter = new PooledArrayBufferWriter();
            WriteHandshake(version, bufferWriter.GetMessageWriter());

            // Prepend size.
            // TODO: Cleanup.
            var buf = bufferWriter.GetWrittenMemory();
            var size = buf.Length - PooledArrayBufferWriter.ReservedPrefixSize;
            var resBuf = buf.Slice(PooledArrayBufferWriter.ReservedPrefixSize - 4);
            WriteMessageSize(resBuf, size);

            await stream.WriteAsync(resBuf).ConfigureAwait(false);
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

        private static unsafe void WriteMessageSize(Memory<byte> target, int size)
        {
            fixed (byte* bufPtr = target.Span)
            {
                *(int*)bufPtr = IPAddress.HostToNetworkOrder(size);
            }
        }

        private async ValueTask SendRequestAsync(PooledArrayBufferWriter? request, ClientOp op, long requestId)
        {
            await _sendLock.WaitAsync(_disposeTokenSource.Token).ConfigureAwait(false);

            try
            {
                // TODO: Optimize prefix handling with a predefined reusable buffer. Remove prefix handling from PooledArrayBufferWriter.
                // TODO: If there is a body, we can use it to write prefix and reduce socket calls.
                var prefixBytes = WritePrefix();
                var body = request?.GetWrittenMemory().Slice(PooledArrayBufferWriter.ReservedPrefixSize);

                var size = prefixBytes.Length + (body?.Length ?? 0);

                var sizeBytes = WriteSize(size);

                await _stream.WriteAsync(sizeBytes, _disposeTokenSource.Token).ConfigureAwait(false);
                await _stream.WriteAsync(prefixBytes, _disposeTokenSource.Token).ConfigureAwait(false);

                if (body != null)
                {
                    await _stream.WriteAsync(body.Value, _disposeTokenSource.Token).ConfigureAwait(false);
                }
            }
            finally
            {
                _sendLock.Release();
            }

            ReadOnlyMemory<byte> WritePrefix()
            {
                var buf = new ArrayBufferWriter<byte>(10);
                var prefixWriter = new MessagePackWriter(buf);
                prefixWriter.Write((int)op);
                prefixWriter.Write(requestId);
                prefixWriter.Flush();

                return buf.WrittenMemory;
            }

            static unsafe ReadOnlyMemory<byte> WriteSize(int size)
            {
                var b = new byte[4];

                fixed (byte* bufPtr = &b[0])
                {
                    *(int*)bufPtr = IPAddress.HostToNetworkOrder(size);
                }

                return b;
            }
        }

        [SuppressMessage(
            "Microsoft.Design",
            "CA1031:DoNotCatchGeneralExceptionTypes",
            Justification = "Any exception in receive loop should be handled.")]
        private async Task RunReceiveLoop(CancellationToken cancellationToken)
        {
            // Reuse the same array for all responses.
            var messageSizeBytes = new byte[4];

            try
            {
                while (!cancellationToken.IsCancellationRequested)
                {
                    PooledBuffer response = await ReadResponseAsync(_stream, messageSizeBytes, cancellationToken).ConfigureAwait(false);

                    // Invoke response handler in another thread to continue the receive loop.
                    // Response buffer should be disposed by the task handler.
                    ThreadPool.QueueUserWorkItem(r => HandleResponse((PooledBuffer)r), response);
                }
            }
            catch (Exception e)
            {
                const string message = "Exception while reading from socket. Connection closed.";

                _logger?.Error(message, e);
                Dispose(new IgniteClientException(message, e));
            }
        }

        private void HandleResponse(PooledBuffer response)
        {
            var reader = response.GetReader();

            var responseType = (ServerMessageType)reader.ReadInt32();

            if (responseType != ServerMessageType.Response)
            {
                // Notifications are not used for now.
                return;
            }

            var requestId = reader.ReadInt64();

            if (!_requests.TryRemove(requestId, out var taskCompletionSource))
            {
                var message = $"Unexpected response ID ({requestId}) received from the server, closing the socket.";
                _logger?.Error(message);
                Dispose(new IgniteClientException(message));

                return;
            }

            var exception = ReadError(ref reader);

            if (exception != null)
            {
                response.Dispose();
                taskCompletionSource.SetException(exception);
            }
            else
            {
                var resultBuffer = response.Slice((int)reader.Consumed);

                taskCompletionSource.SetResult(resultBuffer);
            }
        }

        /// <summary>
        /// Disposes this socket and completes active requests with the specified exception.
        /// </summary>
        /// <param name="ex">Exception to set for pending requests.</param>
        private void Dispose(Exception ex)
        {
            _disposeTokenSource.Cancel();
            _stream.Dispose();

            while (!_requests.IsEmpty)
            {
                foreach (var reqId in _requests.Keys.ToArray())
                {
                    if (_requests.TryRemove(reqId, out var req) && req != null)
                    {
                        req.TrySetException(ex);
                    }
                }
            }
        }
    }
}
