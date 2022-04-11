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
    using Network;
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

        /** Minimum supported heartbeat interval. */
        private static readonly TimeSpan MinRecommendedHeartbeatInterval = TimeSpan.FromMilliseconds(500);

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

        /** Heartbeat timer. */
        private readonly Timer _heartbeatTimer;

        /** Effective heartbeat interval. */
        private readonly TimeSpan _heartbeatInterval;

        /** Logger. */
        private readonly IIgniteLogger? _logger;

        /** Pre-allocated buffer for message size + op code + request id. To be used under <see cref="_sendLock"/>. */
        private readonly byte[] _prefixBuffer = new byte[PooledArrayBufferWriter.ReservedPrefixSize];

        /** Request id generator. */
        private long _requestId;

        /** Exception that caused this socket to close. */
        private volatile Exception? _exception;

        /// <summary>
        /// Initializes a new instance of the <see cref="ClientSocket"/> class.
        /// </summary>
        /// <param name="stream">Network stream.</param>
        /// <param name="configuration">Configuration.</param>
        /// <param name="connectionContext">Connection context.</param>
        private ClientSocket(NetworkStream stream, IgniteClientConfiguration configuration, ConnectionContext connectionContext)
        {
            _stream = stream;
            ConnectionContext = connectionContext;
            _logger = configuration.Logger.GetLogger(GetType());

            _heartbeatInterval = GetHeartbeatInterval(configuration.HeartbeatInterval, connectionContext.IdleTimeout, _logger);

            // ReSharper disable once AsyncVoidLambda (timer callback)
            _heartbeatTimer = new Timer(
                callback: async _ => await SendHeartbeatAsync().ConfigureAwait(false),
                state: null,
                dueTime: _heartbeatInterval,
                period: TimeSpan.FromMilliseconds(-1));

            // Because this call is not awaited, execution of the current method continues before the call is completed.
            // Receive loop runs in the background and should not be awaited.
            _ = RunReceiveLoop(_disposeTokenSource.Token);
        }

        /// <summary>
        /// Gets a value indicating whether this socket is disposed.
        /// </summary>
        public bool IsDisposed => _disposeTokenSource.IsCancellationRequested;

        /// <summary>
        /// Gets the connection context.
        /// </summary>
        public ConnectionContext ConnectionContext { get; }

        /// <summary>
        /// Connects the socket to the specified endpoint and performs handshake.
        /// </summary>
        /// <param name="endPoint">Specific endpoint to connect to.</param>
        /// <param name="configuration">Configuration.</param>
        /// <returns>A <see cref="Task{TResult}"/> representing the result of the asynchronous operation.</returns>
        [SuppressMessage(
            "Microsoft.Reliability",
            "CA2000:Dispose objects before losing scope",
            Justification = "NetworkStream is returned from this method in the socket.")]
        public static async Task<ClientSocket> ConnectAsync(IPEndPoint endPoint, IgniteClientConfiguration configuration)
        {
            var socket = new Socket(SocketType.Stream, ProtocolType.Tcp)
            {
                NoDelay = true
            };

            try
            {
                var logger = configuration.Logger.GetLogger(typeof(ClientSocket));

                await socket.ConnectAsync(endPoint).ConfigureAwait(false);
                logger?.Debug($"Socket connection established: {socket.LocalEndPoint} -> {socket.RemoteEndPoint}");

                var stream = new NetworkStream(socket, ownsSocket: true);

                var context = await HandshakeAsync(stream, endPoint).ConfigureAwait(false);
                logger?.Debug($"Handshake succeeded. Server protocol version: {context.Version}, idle timeout: {context.IdleTimeout}");

                return new ClientSocket(stream, configuration, context);
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
            var ex = _exception;

            if (ex != null)
            {
                throw new IgniteClientException("Socket is closed due to an error, examine inner exception for details.", ex);
            }

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
            Dispose(null);
        }

        /// <summary>
        /// Performs the handshake exchange.
        /// </summary>
        /// <param name="stream">Network stream.</param>
        /// <param name="endPoint">Endpoint.</param>
        private static async Task<ConnectionContext> HandshakeAsync(NetworkStream stream, IPEndPoint endPoint)
        {
            await stream.WriteAsync(ProtoCommon.MagicBytes).ConfigureAwait(false);
            await WriteHandshakeAsync(stream, CurrentProtocolVersion).ConfigureAwait(false);

            await stream.FlushAsync().ConfigureAwait(false);

            await CheckMagicBytesAsync(stream).ConfigureAwait(false);

            using var response = await ReadResponseAsync(stream, new byte[4], CancellationToken.None).ConfigureAwait(false);
            return ReadHandshakeResponse(response.GetReader(), endPoint);
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

        private static ConnectionContext ReadHandshakeResponse(MessagePackReader reader, IPEndPoint endPoint)
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

            var idleTimeoutMs = reader.ReadInt64();
            var clusterNodeId = reader.ReadString();
            var clusterNodeName = reader.ReadString();

            reader.Skip(); // Features.
            reader.Skip(); // Extensions.

            return new ConnectionContext(
                serverVer,
                TimeSpan.FromMilliseconds(idleTimeoutMs),
                new ClusterNode(clusterNodeId, clusterNodeName, endPoint));
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

        private static TimeSpan GetHeartbeatInterval(TimeSpan configuredInterval, TimeSpan serverIdleTimeout, IIgniteLogger? logger)
        {
            if (configuredInterval <= TimeSpan.Zero)
            {
                throw new IgniteClientException(
                    $"{nameof(IgniteClientConfiguration)}.{nameof(IgniteClientConfiguration.HeartbeatInterval)} " +
                    "should be greater than zero.");
            }

            if (serverIdleTimeout <= TimeSpan.Zero)
            {
                logger?.Info(
                    $"Server-side IdleTimeout is not set, using configured {nameof(IgniteClientConfiguration)}." +
                    $"{nameof(IgniteClientConfiguration.HeartbeatInterval)}: {configuredInterval}");

                return configuredInterval;
            }

            var recommendedHeartbeatInterval = serverIdleTimeout / 3;

            if (recommendedHeartbeatInterval < MinRecommendedHeartbeatInterval)
            {
                recommendedHeartbeatInterval = MinRecommendedHeartbeatInterval;
            }

            if (configuredInterval < recommendedHeartbeatInterval)
            {
                logger?.Info(
                    $"Server-side IdleTimeout is {serverIdleTimeout}, " +
                    $"using configured {nameof(IgniteClientConfiguration)}." +
                    $"{nameof(IgniteClientConfiguration.HeartbeatInterval)}: " +
                    configuredInterval);

                return configuredInterval;
            }

            logger?.Warn(
                $"Server-side IdleTimeout is {serverIdleTimeout}, configured " +
                $"{nameof(IgniteClientConfiguration)}.{nameof(IgniteClientConfiguration.HeartbeatInterval)} " +
                $"is {configuredInterval}, which is longer than recommended IdleTimeout / 3. " +
                $"Overriding heartbeat interval with max(IdleTimeout / 3, 500ms): {recommendedHeartbeatInterval}");

            return recommendedHeartbeatInterval;
        }

        private async ValueTask SendRequestAsync(PooledArrayBufferWriter? request, ClientOp op, long requestId)
        {
            // Reset heartbeat timer - don't sent heartbeats when connection is active anyway.
            _heartbeatTimer.Change(dueTime: _heartbeatInterval, period: TimeSpan.FromMilliseconds(-1));

            await _sendLock.WaitAsync(_disposeTokenSource.Token).ConfigureAwait(false);

            try
            {
                var prefixMem = _prefixBuffer.AsMemory()[4..];
                var prefixSize = MessagePackUtil.WriteUnsigned(prefixMem, (int)op);
                prefixSize += MessagePackUtil.WriteUnsigned(prefixMem[prefixSize..], requestId);

                if (request != null)
                {
                    var requestBuf = request.GetWrittenMemory();

                    WriteMessageSize(_prefixBuffer, prefixSize + requestBuf.Length - PooledArrayBufferWriter.ReservedPrefixSize);
                    var prefixBytes = _prefixBuffer.AsMemory()[..(prefixSize + 4)];

                    var requestBufStart = PooledArrayBufferWriter.ReservedPrefixSize - prefixBytes.Length;
                    var requestBufWithPrefix = requestBuf.Slice(requestBufStart);

                    // Copy prefix to request buf to avoid extra WriteAsync call for the prefix.
                    prefixBytes.CopyTo(requestBufWithPrefix);

                    await _stream.WriteAsync(requestBufWithPrefix, _disposeTokenSource.Token).ConfigureAwait(false);
                }
                else
                {
                    // Request without body, send only the prefix.
                    WriteMessageSize(_prefixBuffer, prefixSize);
                    var prefixBytes = _prefixBuffer.AsMemory()[..(prefixSize + 4)];
                    await _stream.WriteAsync(prefixBytes, _disposeTokenSource.Token).ConfigureAwait(false);
                }
            }
            finally
            {
                _sendLock.Release();
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
        /// Sends heartbeat message.
        /// </summary>
        [SuppressMessage(
            "Microsoft.Design",
            "CA1031:DoNotCatchGeneralExceptionTypes",
            Justification = "Any heartbeat exception should cause this instance to be disposed with an error.")]
        private async Task SendHeartbeatAsync()
        {
            try
            {
                await DoOutInOpAsync(ClientOp.Heartbeat).ConfigureAwait(false);
            }
            catch (Exception e)
            {
                Dispose(e);
            }
        }

        /// <summary>
        /// Disposes this socket and completes active requests with the specified exception.
        /// </summary>
        /// <param name="ex">Exception that caused this socket to close. Null when socket is closed by the user.</param>
        private void Dispose(Exception? ex)
        {
            if (_disposeTokenSource.IsCancellationRequested)
            {
                return;
            }

            _heartbeatTimer.Dispose();
            _disposeTokenSource.Cancel();
            _exception = ex;
            _stream.Dispose();

            ex ??= new ObjectDisposedException("Connection closed.");

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
