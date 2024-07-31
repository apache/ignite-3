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
    using System.Buffers.Binary;
    using System.Collections.Concurrent;
    using System.Diagnostics;
    using System.Diagnostics.CodeAnalysis;
    using System.IO;
    using System.Linq;
    using System.Net.Security;
    using System.Net.Sockets;
    using System.Threading;
    using System.Threading.Tasks;
    using Buffers;
    using Ignite.Network;
    using Microsoft.Extensions.Logging;
    using Network;
    using Proto;
    using Proto.MsgPack;

    /// <summary>
    /// Wrapper over framework socket for Ignite thin client operations.
    /// </summary>
    // ReSharper disable SuggestBaseTypeForParameter (NetworkStream has more efficient read/write methods).
    internal sealed partial class ClientSocket : IDisposable
    {
        /** General-purpose client type code. */
        private const byte ClientType = 2;

        /** Version 3.0.0. */
        private static readonly ClientProtocolVersion Ver300 = new(3, 0, 0);

        /** Current version. */
        private static readonly ClientProtocolVersion CurrentProtocolVersion = Ver300;

        /** Minimum supported heartbeat interval. */
        private static readonly TimeSpan MinRecommendedHeartbeatInterval = TimeSpan.FromMilliseconds(500);

        /** Socket id for debug logging. */
        private static long _socketId;

        /** Underlying stream. */
        private readonly Stream _stream;

        /** Current async operations, map from request id. */
        private readonly ConcurrentDictionary<long, TaskCompletionSource<PooledBuffer>> _requests = new();

        /** Current notification handlers, map from request id. */
        private readonly ConcurrentDictionary<long, NotificationHandler> _notificationHandlers = new();

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

        /** Dispose lock. */
        private readonly object _disposeLock = new();

        /** Heartbeat timer. */
        private readonly Timer _heartbeatTimer;

        /** Effective heartbeat interval. */
        private readonly TimeSpan _heartbeatInterval;

        /** Socket timeout for handshakes and heartbeats. */
        private readonly TimeSpan _socketTimeout;

        /** Operation timeout for user-initiated requests. */
        private readonly TimeSpan _operationTimeout;

        /** Logger. */
        private readonly ILogger _logger;

        /** Event listener. */
        private readonly IClientSocketEventListener _listener;

        /** Pre-allocated buffer for message size + op code + request id. To be used under <see cref="_sendLock"/>. */
        private readonly byte[] _prefixBuffer = new byte[ProtoCommon.MessagePrefixSize];

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
        /// <param name="listener">Event listener.</param>
        /// <param name="logger">Logger.</param>
        private ClientSocket(
            Stream stream,
            IgniteClientConfiguration configuration,
            ConnectionContext connectionContext,
            IClientSocketEventListener listener,
            ILogger logger)
        {
            _stream = stream;
            ConnectionContext = connectionContext;
            _listener = listener;
            _logger = logger;
            _socketTimeout = configuration.SocketTimeout;
            _operationTimeout = configuration.OperationTimeout;

            MetricsContext = connectionContext.ClusterNode.MetricsContext ??
                             throw new InvalidOperationException("Metrics context is missing.");

            _heartbeatInterval = GetHeartbeatInterval(configuration.HeartbeatInterval, connectionContext.IdleTimeout, _logger);

            // ReSharper disable once AsyncVoidLambda (timer callback)
            _heartbeatTimer = new Timer(
                callback: async _ => await HeartbeatAsync().ConfigureAwait(false),
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
        /// <param name="listener">Event listener.</param>
        /// <returns>A <see cref="Task{TResult}"/> representing the result of the asynchronous operation.</returns>
        [SuppressMessage(
            "Microsoft.Reliability",
            "CA2000:Dispose objects before losing scope",
            Justification = "NetworkStream is returned from this method in the socket.")]
        [SuppressMessage("Maintainability", "CA1508:Avoid dead conditional code", Justification = "False positive")]
        [SuppressMessage("Design", "CA1031:Do not catch general exception types", Justification = "Reviewed")]
        public static async Task<ClientSocket> ConnectAsync(
            SocketEndpoint endPoint,
            IgniteClientConfiguration configuration,
            IClientSocketEventListener listener)
        {
            using var cts = new CancellationTokenSource();
            var logger = configuration.LoggerFactory.CreateLogger(typeof(ClientSocket).FullName! + "-" +
                                                                  Interlocked.Increment(ref _socketId));

            bool connected = false;
            Socket? socket = null;
            Stream? stream = null;

            try
            {
                socket = new Socket(SocketType.Stream, ProtocolType.Tcp)
                {
                    NoDelay = true
                };

                await socket.ConnectAsync(endPoint.EndPoint, cts.Token)
                    .AsTask()
                    .WaitAsync(configuration.SocketTimeout, cts.Token)
                    .ConfigureAwait(false);

                logger.LogConnectionEstablishedDebug(socket.RemoteEndPoint);

                Metrics.ConnectionsEstablished.Add(1, endPoint.MetricsContext.Tags);
                Metrics.ConnectionsActiveIncrement();
                connected = true;

                stream = new NetworkStream(socket, ownsSocket: true);

                if (configuration.SslStreamFactory is { } sslStreamFactory &&
                    await sslStreamFactory.CreateAsync(stream, endPoint.Host, cts.Token)
                        .WaitAsync(configuration.SocketTimeout, cts.Token)
                        .ConfigureAwait(false) is { } sslStream)
                {
                    stream = sslStream;
                    logger.LogSslConnectionEstablishedDebug(socket.RemoteEndPoint, sslStream.NegotiatedCipherSuite);
                }

                var context = await HandshakeAsync(stream, endPoint, configuration, listener, cts.Token)
                    .WaitAsync(configuration.SocketTimeout, cts.Token)
                    .ConfigureAwait(false);

                logger.LogHandshakeSucceededDebug(socket.RemoteEndPoint, context);

                return new ClientSocket(stream, configuration, context, listener, logger);
            }
            catch (Exception ex)
            {
                try
                {
                    cts.Cancel();
                    socket?.Dispose();

                    if (stream != null)
                    {
                        await stream.DisposeAsync().ConfigureAwait(false);
                    }
                }
                catch (Exception disposeEx)
                {
                    logger.LogFailedToDisposeSocketAfterFailedConnectionAttemptWarn(disposeEx, disposeEx.Message);
                }

                logger.LogConnectionFailedBeforeOrDuringHandshakeWarn(ex, endPoint.EndPoint, ex.Message);

                if (ex.GetBaseException() is TimeoutException)
                {
                    Metrics.HandshakesFailedTimeout.Add(1, endPoint.MetricsContext.Tags);
                }
                else
                {
                    Metrics.HandshakesFailed.Add(1, endPoint.MetricsContext.Tags);
                }

                if (connected)
                {
                    Metrics.ConnectionsActiveDecrement();
                }

                throw new IgniteClientConnectionException(
                    ErrorGroups.Client.Connection,
                    "Failed to connect to endpoint: " + endPoint.EndPoint,
                    ex);
            }
        }

        /// <summary>
        /// Performs an in-out operation.
        /// </summary>
        /// <param name="clientOp">Client op code.</param>
        /// <param name="request">Request data.</param>
        /// <param name="expectNotifications">Whether to expect notifications as a result of the operation.</param>
        /// <returns>Response data.</returns>
        public Task<PooledBuffer> DoOutInOpAsync(
            ClientOp clientOp,
            PooledArrayBuffer? request = null,
            bool expectNotifications = false) =>
            DoOutInOpAsyncInternal(clientOp, request, expectNotifications)
                .WaitAsync(_operationTimeout);

        /// <inheritdoc/>
        public void Dispose()
        {
            Dispose(null);
        }

        /// <summary>
        /// Sends heartbeat message.
        /// </summary>
        /// <param name="payload">Optional payload. Ignored by the server, can be used for benchmarking.</param>
        /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
        [SuppressMessage(
            "Microsoft.Design",
            "CA1031:DoNotCatchGeneralExceptionTypes",
            Justification = "Any heartbeat exception should cause this instance to be disposed with an error.")]
        internal async Task HeartbeatAsync(PooledArrayBuffer? payload = null)
        {
            try
            {
                using var buf = await DoOutInOpAsync(ClientOp.Heartbeat, payload)
                    .WaitAsync(_socketTimeout)
                    .ConfigureAwait(false);
            }
            catch (Exception e)
            {
                var message = "Heartbeat failed: " + e.Message;
                _logger.LogHeartbeatError(e, message);

                Dispose(new IgniteClientConnectionException(ErrorGroups.Client.Connection, message, e));
            }
        }

        /// <summary>
        /// Performs the handshake exchange.
        /// </summary>
        /// <param name="stream">Network stream.</param>
        /// <param name="endPoint">Endpoint.</param>
        /// <param name="configuration">Configuration.</param>
        /// <param name="listener">Client socket event listener.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        private static async Task<ConnectionContext> HandshakeAsync(
            Stream stream,
            SocketEndpoint endPoint,
            IgniteClientConfiguration configuration,
            IClientSocketEventListener listener,
            CancellationToken cancellationToken)
        {
            await stream.WriteAsync(ProtoCommon.MagicBytes, cancellationToken).ConfigureAwait(false);
            await WriteHandshakeAsync(stream, CurrentProtocolVersion, configuration, endPoint.MetricsContext, cancellationToken)
                .ConfigureAwait(false);

            await stream.FlushAsync(cancellationToken).ConfigureAwait(false);

            await CheckMagicBytesAsync(stream, endPoint.MetricsContext, cancellationToken).ConfigureAwait(false);

            using var response = await ReadResponseAsync(stream, new byte[4], endPoint.MetricsContext, CancellationToken.None)
                .ConfigureAwait(false);

            return ReadHandshakeResponse(response.GetReader(), endPoint, GetSslInfo(stream), listener);
        }

        private static async ValueTask CheckMagicBytesAsync(
            Stream stream,
            MetricsContext metricsContext,
            CancellationToken cancellationToken)
        {
            var responseMagic = ByteArrayPool.Rent(ProtoCommon.MagicBytes.Length);

            try
            {
                await ReceiveBytesAsync(stream, responseMagic, ProtoCommon.MagicBytes.Length, metricsContext, cancellationToken)
                    .ConfigureAwait(false);

                for (var i = 0; i < ProtoCommon.MagicBytes.Length; i++)
                {
                    if (responseMagic[i] != ProtoCommon.MagicBytes[i])
                    {
                        throw new IgniteClientConnectionException(
                            ErrorGroups.Client.Protocol,
                            "Invalid magic bytes returned from the server: " + BitConverter.ToString(responseMagic));
                    }
                }
            }
            finally
            {
                ByteArrayPool.Return(responseMagic);
            }
        }

        private static ConnectionContext ReadHandshakeResponse(
            MsgPackReader reader,
            SocketEndpoint endPoint,
            ISslInfo? sslInfo,
            IClientSocketEventListener listener)
        {
            var serverVer = new ClientProtocolVersion(reader.ReadInt16(), reader.ReadInt16(), reader.ReadInt16());

            if (serverVer != CurrentProtocolVersion)
            {
                throw new IgniteClientConnectionException(ErrorGroups.Client.Protocol, "Unexpected server version: " + serverVer);
            }

            if (!reader.TryReadNil())
            {
                throw ReadError(ref reader);
            }

            var idleTimeoutMs = reader.ReadInt64();
            var clusterNodeId = reader.ReadString();
            var clusterNodeName = reader.ReadString();

            var clusterId = reader.ReadGuid();
            var clusterName = reader.ReadString();

            var observableTimestamp = reader.ReadInt64();
            listener.OnObservableTimestampChanged(observableTimestamp);

            // Cluster version.
            reader.Skip(); // Major.
            reader.Skip(); // Minor.
            reader.Skip(); // Maintenance.
            reader.Skip(); // Patch.
            reader.Skip(); // Pre-release.

            reader.Skip(); // Features.
            reader.Skip(); // Extensions.

            return new ConnectionContext(
                serverVer,
                TimeSpan.FromMilliseconds(idleTimeoutMs),
                new ClusterNode(clusterNodeId, clusterNodeName, endPoint.EndPoint, endPoint.MetricsContext),
                clusterId,
                clusterName,
                sslInfo);
        }

        private static IgniteException ReadError(ref MsgPackReader reader)
        {
            Guid traceId = reader.TryReadNil() ? Guid.NewGuid() : reader.ReadGuid();
            int code = reader.TryReadNil() ? 65537 : reader.ReadInt32();
            string className = reader.ReadString();
            string? message = reader.ReadStringNullable();
            string? javaStackTrace = reader.ReadStringNullable();
            var ex = ExceptionMapper.GetException(traceId, code, className, message, javaStackTrace);

            int extensionCount = reader.TryReadNil() ? 0 : reader.ReadInt32();
            for (int i = 0; i < extensionCount; i++)
            {
                var key = reader.ReadString();
                if (key == ErrorExtensions.ExpectedSchemaVersion)
                {
                    ex.Data[key] = reader.ReadInt32();
                }
                else
                {
                    reader.Skip(); // Unknown extension - ignore.
                }
            }

            return ex;
        }

        private static async ValueTask<PooledBuffer> ReadResponseAsync(
            Stream stream,
            byte[] messageSizeBytes,
            MetricsContext metricsContext,
            CancellationToken cancellationToken)
        {
            var size = await ReadMessageSizeAsync(stream, messageSizeBytes, metricsContext, cancellationToken).ConfigureAwait(false);

            var bytes = ByteArrayPool.Rent(size);

            try
            {
                await ReceiveBytesAsync(stream, bytes, size, metricsContext, cancellationToken).ConfigureAwait(false);

                return new PooledBuffer(bytes, 0, size);
            }
            catch (Exception)
            {
                ByteArrayPool.Return(bytes);

                throw;
            }
        }

        private static async Task<int> ReadMessageSizeAsync(
            Stream stream,
            byte[] buffer,
            MetricsContext metricsContext,
            CancellationToken cancellationToken)
        {
            const int messageSizeByteCount = 4;
            Debug.Assert(buffer.Length >= messageSizeByteCount, "buffer.Length >= messageSizeByteCount");

            await ReceiveBytesAsync(stream, buffer, messageSizeByteCount, metricsContext, cancellationToken).ConfigureAwait(false);

            return ReadMessageSize(buffer);
        }

        private static async Task ReceiveBytesAsync(
            Stream stream,
            byte[] buffer,
            int size,
            MetricsContext metricsContext,
            CancellationToken cancellationToken)
        {
            int received = 0;

            while (received < size)
            {
                var res = await stream.ReadAsync(buffer.AsMemory(received, size - received), cancellationToken).ConfigureAwait(false);

                if (res == 0)
                {
                    // Disconnected.
                    throw new IgniteClientConnectionException(
                        ErrorGroups.Client.Connection,
                        "Connection lost (failed to read data from socket)",
                        new SocketException((int) SocketError.ConnectionAborted));
                }

                received += res;

                AddBytesReceived(res, metricsContext);
            }
        }

        private static async ValueTask WriteHandshakeAsync(
            Stream stream,
            ClientProtocolVersion version,
            IgniteClientConfiguration configuration,
            MetricsContext metricsContext,
            CancellationToken token)
        {
            using var bufferWriter = new PooledArrayBuffer(prefixSize: ProtoCommon.MessagePrefixSize);
            WriteHandshake(bufferWriter.MessageWriter, version, configuration);

            // Prepend size.
            var buf = bufferWriter.GetWrittenMemory();
            var size = buf.Length - ProtoCommon.MessagePrefixSize;
            var resBuf = buf.Slice(ProtoCommon.MessagePrefixSize - 4);
            WriteMessageSize(resBuf, size);

            await stream.WriteAsync(resBuf, token).ConfigureAwait(false);

            AddBytesSent(resBuf.Length + ProtoCommon.MagicBytes.Length, metricsContext);
        }

        private static void WriteHandshake(MsgPackWriter w, ClientProtocolVersion version, IgniteClientConfiguration configuration)
        {
            // Version.
            w.Write(version.Major);
            w.Write(version.Minor);
            w.Write(version.Patch);

            w.Write(ClientType); // Client type: general purpose.

            w.WriteBinaryHeader(0); // Features.

            if (configuration.Authenticator != null)
            {
                w.Write(3); // Extensions.

                w.Write(HandshakeExtensions.AuthenticationType);
                w.Write(configuration.Authenticator.Type);

                w.Write(HandshakeExtensions.AuthenticationIdentity);
                w.Write((string?)configuration.Authenticator.Identity);

                w.Write(HandshakeExtensions.AuthenticationSecret);
                w.Write((string?)configuration.Authenticator.Secret);
            }
            else
            {
                w.Write(0); // Extensions.
            }
        }

        private static void WriteMessageSize(Memory<byte> target, int size) =>
            BinaryPrimitives.WriteInt32BigEndian(target.Span, size);

        private static int ReadMessageSize(Span<byte> responseLenBytes) => BinaryPrimitives.ReadInt32BigEndian(responseLenBytes);

        private static TimeSpan GetHeartbeatInterval(TimeSpan configuredInterval, TimeSpan serverIdleTimeout, ILogger logger)
        {
            if (configuredInterval <= TimeSpan.Zero)
            {
                throw new IgniteClientException(
                    ErrorGroups.Client.Configuration,
                    $"{nameof(IgniteClientConfiguration)}.{nameof(IgniteClientConfiguration.HeartbeatInterval)} should be greater than zero.");
            }

            if (serverIdleTimeout <= TimeSpan.Zero)
            {
                logger.LogServerSizeIdleTimeoutNotSetInfo(configuredInterval);

                return configuredInterval;
            }

            var recommendedHeartbeatInterval = serverIdleTimeout / 3;

            if (recommendedHeartbeatInterval < MinRecommendedHeartbeatInterval)
            {
                recommendedHeartbeatInterval = MinRecommendedHeartbeatInterval;
            }

            if (configuredInterval < recommendedHeartbeatInterval)
            {
                logger.LogServerSideIdleTimeoutIgnoredInfo(serverIdleTimeout, configuredInterval);

                return configuredInterval;
            }

            logger.LogServerSideIdleTimeoutOverridesConfiguredHeartbeatIntervalWarn(
                serverIdleTimeout, configuredInterval, recommendedHeartbeatInterval);

            return recommendedHeartbeatInterval;
        }

        private static ISslInfo? GetSslInfo(Stream stream) =>
            stream is SslStream sslStream
                ? new SslInfo(
                    sslStream.TargetHostName,
                    sslStream.NegotiatedCipherSuite.ToString(),
                    sslStream.IsMutuallyAuthenticated,
                    sslStream.LocalCertificate,
                    sslStream.RemoteCertificate,
                    sslStream.SslProtocol)
                : null;

        private async Task<PooledBuffer> DoOutInOpAsyncInternal(
            ClientOp clientOp,
            PooledArrayBuffer? request = null,
            bool expectNotifications = false)
        {
            var ex = _exception;

            if (ex != null)
            {
                throw new IgniteClientConnectionException(
                    ErrorGroups.Client.Connection,
                    "Socket is closed due to an error, examine inner exception for details.",
                    ex);
            }

            if (_disposeTokenSource.IsCancellationRequested)
            {
                throw new IgniteClientConnectionException(
                    ErrorGroups.Client.Connection,
                    "Socket is disposed.",
                    new ObjectDisposedException(nameof(ClientSocket)));
            }

            var requestId = Interlocked.Increment(ref _requestId);
            var taskCompletionSource = new TaskCompletionSource<PooledBuffer>();
            _requests[requestId] = taskCompletionSource;

            NotificationHandler? notificationHandler = null;
            if (expectNotifications)
            {
                notificationHandler = new NotificationHandler();
                _notificationHandlers[requestId] = notificationHandler;
            }

            Metrics.RequestsActiveIncrement();

            try
            {
                await SendRequestAsync(request, clientOp, requestId).ConfigureAwait(false);
                PooledBuffer resBuf = await taskCompletionSource.Task.ConfigureAwait(false);
                resBuf.Metadata = notificationHandler;

                return resBuf;
            }
            catch (Exception e)
            {
                if (_requests.TryRemove(requestId, out _))
                {
                    AddFailedRequest();
                    Metrics.RequestsActiveDecrement();
                }

                _notificationHandlers.TryRemove(requestId, out _);

                if (e is OperationCanceledException or ObjectDisposedException)
                {
                    // Canceled task means Dispose was called.
                    throw new IgniteClientConnectionException(ErrorGroups.Client.Connection, "Connection closed.", e);
                }

                throw;
            }
        }

        [SuppressMessage(
            "Microsoft.Design",
            "CA1031:DoNotCatchGeneralExceptionTypes",
            Justification = "Any exception during socket write should be handled to close the socket.")]
        private async ValueTask SendRequestAsync(PooledArrayBuffer? request, ClientOp op, long requestId)
        {
            // Reset heartbeat timer - don't sent heartbeats when connection is active anyway.
            _heartbeatTimer.Change(dueTime: _heartbeatInterval, period: TimeSpan.FromMilliseconds(-1));

            _logger.LogSendingRequestTrace(requestId, op, ConnectionContext.ClusterNode.Address);

            await _sendLock.WaitAsync(_disposeTokenSource.Token).ConfigureAwait(false);

            try
            {
                var prefixMem = _prefixBuffer.AsMemory()[4..];
                var prefixSize = MsgPackWriter.WriteUnsigned(prefixMem.Span, (ulong)op);
                prefixSize += MsgPackWriter.WriteUnsigned(prefixMem[prefixSize..].Span, (ulong)requestId);

                if (request != null)
                {
                    var requestBuf = request.GetWrittenMemory();

                    WriteMessageSize(_prefixBuffer, prefixSize + requestBuf.Length - ProtoCommon.MessagePrefixSize);
                    var prefixBytes = _prefixBuffer.AsMemory()[..(prefixSize + 4)];

                    var requestBufStart = ProtoCommon.MessagePrefixSize - prefixBytes.Length;
                    var requestBufWithPrefix = requestBuf.Slice(requestBufStart);

                    // Copy prefix to request buf to avoid extra WriteAsync call for the prefix.
                    prefixBytes.CopyTo(requestBufWithPrefix);

                    await _stream.WriteAsync(requestBufWithPrefix, _disposeTokenSource.Token).ConfigureAwait(false);

                    AddBytesSent(requestBufWithPrefix.Length);
                }
                else
                {
                    // Request without body, send only the prefix.
                    WriteMessageSize(_prefixBuffer, prefixSize);
                    var prefixBytes = _prefixBuffer.AsMemory()[..(prefixSize + 4)];
                    await _stream.WriteAsync(prefixBytes, _disposeTokenSource.Token).ConfigureAwait(false);

                    AddBytesSent(prefixBytes.Length);
                }

                Metrics.RequestsSent.Add(1, MetricsContext.Tags);
            }
            catch (Exception e)
            {
                var message = "Exception while writing to socket, connection closed: " + e.Message;

                _logger.LogSocketIoError(e, message);
                var connEx = new IgniteClientConnectionException(ErrorGroups.Client.Connection, message, new SocketException());

                Dispose(connEx);
                throw connEx;
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
                    PooledBuffer response = await ReadResponseAsync(
                        _stream, messageSizeBytes, MetricsContext, cancellationToken).ConfigureAwait(false);

                    // Invoke response handler in another thread to continue the receive loop.
                    // Response buffer should be disposed by the task handler.
                    ThreadPool.QueueUserWorkItem<(ClientSocket Socket, PooledBuffer Buf)>(
                        callBack: static r => r.Socket.HandleResponse(r.Buf),
                        state: (this, response),
                        preferLocal: true);
                }
            }
            catch (Exception e)
            {
                var message = "Exception while reading from socket, connection closed: " + e.Message;

                _logger.LogSocketIoError(e, message);
                Dispose(new IgniteClientConnectionException(ErrorGroups.Client.Connection, message, e));
            }
        }

        [SuppressMessage("Design", "CA1031:Do not catch general exception types", Justification = "Thread root.")]
        private void HandleResponse(PooledBuffer response)
        {
            bool handled = false;

            try
            {
                handled = HandleResponseInner(response);
            }
            catch (IgniteClientConnectionException e)
            {
                Dispose(e);
            }
            catch (Exception e)
            {
                var message = "Exception while handling response, connection closed: " + e.Message;
                Dispose(new IgniteClientConnectionException(ErrorGroups.Client.Connection, message, e));
            }
            finally
            {
                if (!handled)
                {
                    response.Dispose();
                }
            }
        }

        /// <summary>
        /// Handles a server response.
        /// </summary>
        /// <param name="response">Response buffer.</param>
        /// <returns>
        /// A value indicating whether the response buffer was passed on to the final handler and does not need to be disposed.
        /// </returns>
        private bool HandleResponseInner(PooledBuffer response)
        {
            var reader = response.GetReader();

            var requestId = reader.ReadInt64();
            var flags = (ResponseFlags)reader.ReadInt32();

            _logger.LogReceivedResponseTrace(requestId, flags, ConnectionContext.ClusterNode.Address);

            HandlePartitionAssignmentChange(flags, ref reader);
            HandleObservableTimestamp(ref reader);

            var exception = flags.HasFlag(ResponseFlags.Error) ? ReadError(ref reader) : null;
            response.Position += reader.Consumed;

            if (flags.HasFlag(ResponseFlags.Notification))
            {
                return HandleNotification(requestId, exception, response);
            }

            if (!_requests.TryRemove(requestId, out var taskCompletionSource))
            {
                var message = $"Unexpected response ID ({requestId}) received from the server " +
                              $"[remoteAddress={ConnectionContext.ClusterNode.Address}], closing the socket.";

                _logger.LogUnexpectedResponseIdError(null, message);
                throw new IgniteClientConnectionException(ErrorGroups.Client.Protocol, message);
            }

            Metrics.RequestsActiveDecrement();

            if (exception != null)
            {
                AddFailedRequest();

                taskCompletionSource.TrySetException(exception);
                return false;
            }

            Metrics.RequestsCompleted.Add(1, MetricsContext.Tags);

            return taskCompletionSource.TrySetResult(response);
        }

        /// <summary>
        /// Handles a server notification.
        /// </summary>
        /// <param name="requestId">Request id.</param>
        /// <param name="exception">Exception.</param>
        /// <param name="response">Response buffer.</param>
        /// <returns>
        /// A value indicating whether the response buffer was passed on to the final handler and does not need to be disposed.
        /// </returns>
        private bool HandleNotification(long requestId, Exception? exception, PooledBuffer response)
        {
            if (!_notificationHandlers.TryRemove(requestId, out var notificationHandler))
            {
                var message = $"Unexpected notification ID ({requestId}) received from the server " +
                              $"[remoteAddress={ConnectionContext.ClusterNode.Address}], closing the socket.";

                _logger.LogUnexpectedResponseIdError(null, message);
                throw new IgniteClientConnectionException(ErrorGroups.Client.Protocol, message);
            }

            if (exception != null)
            {
                notificationHandler.TrySetException(exception);
                return false;
            }

            return notificationHandler.TrySetResult(response);
        }

        private void HandleObservableTimestamp(ref MsgPackReader reader)
        {
            var observableTimestamp = reader.ReadInt64();
            _listener.OnObservableTimestampChanged(observableTimestamp);
        }

        private void HandlePartitionAssignmentChange(ResponseFlags flags, ref MsgPackReader reader)
        {
            if (flags.HasFlag(ResponseFlags.PartitionAssignmentChanged))
            {
                long timestamp = reader.ReadInt64();

                _logger.LogPartitionAssignmentChangeNotificationInfo(ConnectionContext.ClusterNode.Address, timestamp);

                _listener.OnAssignmentChanged(timestamp);
            }
        }

        /// <summary>
        /// Disposes this socket and completes active requests with the specified exception.
        /// </summary>
        /// <param name="ex">Exception that caused this socket to close. Null when socket is closed by the user.</param>
        private void Dispose(Exception? ex)
        {
            lock (_disposeLock)
            {
                if (_disposeTokenSource.IsCancellationRequested)
                {
                    return;
                }

                _disposeTokenSource.Cancel();

                if (ex != null)
                {
                    _logger.LogConnectionClosedWithErrorWarn(ex, ConnectionContext.ClusterNode.Address, ex.Message);

                    Metrics.ConnectionsLost.Add(1, MetricsContext.Tags);

                    if (ex.GetBaseException() is TimeoutException)
                    {
                        Metrics.ConnectionsLostTimeout.Add(1, MetricsContext.Tags);
                    }
                }
                else
                {
                    _logger.LogConnectionClosedGracefullyDebug(ConnectionContext.ClusterNode.Address);
                }

                _heartbeatTimer.Dispose();
                _exception = ex;
                _stream.Dispose();

                ex ??= new IgniteClientConnectionException(ErrorGroups.Client.Connection, "Connection closed.");

                while (!_requests.IsEmpty)
                {
                    foreach (var reqId in _requests.Keys.ToArray())
                    {
                        if (_requests.TryRemove(reqId, out var req))
                        {
                            req.TrySetException(ex);
                            Metrics.RequestsActiveDecrement();
                        }
                    }
                }

                while (!_notificationHandlers.IsEmpty)
                {
                    foreach (var reqId in _notificationHandlers.Keys.ToArray())
                    {
                        if (_notificationHandlers.TryRemove(reqId, out var notificationHandler))
                        {
                            notificationHandler.TrySetException(ex);
                        }
                    }
                }

                Metrics.ConnectionsActiveDecrement();
            }
        }
    }
}
