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
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Diagnostics.CodeAnalysis;
    using System.IO;
    using System.Linq;
    using System.Net;
    using System.Net.Sockets;
    using System.Threading;
    using System.Threading.Tasks;
    using Buffers;
    using Common;
    using Ignite.Network;
    using Microsoft.Extensions.Logging;
    using Network;
    using Proto;
    using Transactions;

    /// <summary>
    /// Client socket wrapper with reconnect/failover functionality.
    /// </summary>
    internal sealed class ClientFailoverSocket : IDisposable, IClientSocketEventListener
    {
        private const string ExceptionDataEndpoint = "Endpoint";

        /** Current global endpoint index for Round-robin. */
        private static long _globalEndPointIndex;

        /** Logger. */
        private readonly ILogger _logger;

        /** Endpoints with corresponding hosts - from configuration. */
        private readonly IReadOnlyList<SocketEndpoint> _endpoints;

        /** Cluster node unique name to endpoint map. */
        private readonly ConcurrentDictionary<string, SocketEndpoint> _endpointsByName = new();

        /** Socket connection lock. */
        [SuppressMessage(
            "Microsoft.Design",
            "CA2213:DisposableFieldsShouldBeDisposed",
            Justification = "WaitHandle is not used in SemaphoreSlim, no need to dispose.")]
        private readonly SemaphoreSlim _socketLock = new(1);

        /** Disposed flag. */
        private volatile bool _disposed;

        /** Local topology assignment version. Instead of using event handlers to notify all tables about assignment change,
         * the table will compare its version with channel version to detect an update. */
        private long _assignmentTimestamp;

        /** Cluster id from the first handshake. */
        private Guid? _clusterId;

        /** Local index for round-robin balancing within this FailoverSocket. */
        private long _endPointIndex = Interlocked.Increment(ref _globalEndPointIndex);

        /** Observable timestamp. */
        private long _observableTimestamp;

        /// <summary>
        /// Initializes a new instance of the <see cref="ClientFailoverSocket"/> class.
        /// </summary>
        /// <param name="configuration">Client configuration.</param>
        /// <param name="logger">Logger.</param>
        private ClientFailoverSocket(IgniteClientConfigurationInternal configuration, ILogger logger)
        {
            if (configuration.Configuration.Endpoints.Count == 0)
            {
                throw new IgniteClientException(
                    ErrorGroups.Client.Configuration,
                    $"Invalid {nameof(IgniteClientConfiguration)}: {nameof(IgniteClientConfiguration.Endpoints)} is empty. Nowhere to connect.");
            }

            _logger = logger;
            _endpoints = GetIpEndPoints(configuration.Configuration).ToList();

            Configuration = configuration;
        }

        /// <summary>
        /// Gets the configuration.
        /// </summary>
        public IgniteClientConfigurationInternal Configuration { get; }

        /// <summary>
        /// Gets the partition assignment timestamp.
        /// </summary>
        public long PartitionAssignmentTimestamp => Interlocked.Read(ref _assignmentTimestamp);

        /// <summary>
        /// Gets the observable timestamp.
        /// </summary>
        public long ObservableTimestamp => Interlocked.Read(ref _observableTimestamp);

        /// <summary>
        /// Gets the client ID.
        /// </summary>
        public Guid ClientId { get; } = Guid.NewGuid();

        /// <summary>
        /// Gets a value indicating whether the socket is disposed.
        /// </summary>
        public bool IsDisposed => _disposed;

        /// <summary>
        /// Gets the logger.
        /// </summary>
        public ILogger Logger => _logger;

        /// <summary>
        /// Connects the socket.
        /// </summary>
        /// <param name="configuration">Client configuration.</param>
        /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
        public static async Task<ClientFailoverSocket> ConnectAsync(IgniteClientConfigurationInternal configuration)
        {
            var logger = configuration.Configuration.LoggerFactory.CreateLogger<ClientFailoverSocket>();
            logger.LogClientStartInfo(VersionUtils.InformationalVersion);

            var socket = new ClientFailoverSocket(configuration, logger);

            await socket.GetNextSocketAsync().ConfigureAwait(false);

            // Because this call is not awaited, execution of the current method continues before the call is completed.
            // Secondary connections are established in the background.
            _ = socket.ConnectAllSockets();

            return socket;
        }

        /// <summary>
        /// Resets global endpoint index. For testing purposes only (to make behavior deterministic).
        /// </summary>
        public static void ResetGlobalEndpointIndex() => _globalEndPointIndex = 0;

        /// <summary>
        /// Performs an in-out operation.
        /// </summary>
        /// <param name="clientOp">Client op code.</param>
        /// <param name="request">Request data.</param>
        /// <param name="preferredNode">Preferred node.</param>
        /// <param name="expectNotifications">Whether to expect notifications as a result of the operation.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        /// <returns>Response data and socket.</returns>
        public async Task<PooledBuffer> DoOutInOpAsync(
            ClientOp clientOp,
            PooledArrayBuffer? request = null,
            PreferredNode preferredNode = default,
            bool expectNotifications = false,
            CancellationToken cancellationToken = default)
        {
            var (buffer, _) = await DoOutInOpAndGetSocketAsync(
                    clientOp,
                    tx: null,
                    request,
                    preferredNode,
                    retryPolicyOverride: null,
                    expectNotifications,
                    cancellationToken)
                .ConfigureAwait(false);

            return buffer;
        }

        /// <summary>
        /// Performs an in-out operation.
        /// </summary>
        /// <param name="clientOp">Client op code.</param>
        /// <param name="tx">Transaction.</param>
        /// <param name="request">Request data.</param>
        /// <param name="preferredNode">Preferred node.</param>
        /// <param name="retryPolicyOverride">Retry policy.</param>
        /// <param name="expectNotifications">Whether to expect notifications as a result of the operation.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        /// <returns>Response data and socket.</returns>
        public async Task<(PooledBuffer Buffer, ClientSocket Socket)> DoOutInOpAndGetSocketAsync(
            ClientOp clientOp,
            Transaction? tx = null,
            PooledArrayBuffer? request = null,
            PreferredNode preferredNode = default,
            IRetryPolicy? retryPolicyOverride = null,
            bool expectNotifications = false,
            CancellationToken cancellationToken = default)
        {
            if (tx != null)
            {
                if (tx.FailoverSocket != this)
                {
                    throw new IgniteClientException(ErrorGroups.Client.Connection, "Specified transaction belongs to a different IgniteClient instance.");
                }

                // Use tx-specific socket without retry and failover.
                var buffer = await tx.Socket.DoOutInOpAsync(clientOp, request, expectNotifications, cancellationToken).ConfigureAwait(false);
                return (buffer, tx.Socket);
            }

            return await DoWithRetryAsync(
                (clientOp, request, expectNotifications, cancellationToken),
                static (_, arg) => arg.clientOp,
                async static (socket, arg) =>
                {
                    PooledBuffer res = await socket.DoOutInOpAsync(
                        arg.clientOp, arg.request, arg.expectNotifications, arg.cancellationToken).ConfigureAwait(false);

                    return (Buffer: res, Socket: socket);
                },
                preferredNode,
                retryPolicyOverride)
                .ConfigureAwait(false);
        }

        /// <summary>
        /// Performs a socket operation with retry and reconnect.
        /// </summary>
        /// <param name="arg">Func argument.</param>
        /// <param name="opFunc">Client op func.</param>
        /// <param name="func">Result func.</param>
        /// <param name="preferredNode">Preferred node.</param>
        /// <param name="retryPolicyOverride">Retry policy.</param>
        /// <typeparam name="T">Result type.</typeparam>
        /// <typeparam name="TArg">Arg type.</typeparam>
        /// <returns>Result.</returns>
        public async Task<T> DoWithRetryAsync<T, TArg>(
            TArg arg,
            Func<ClientSocket?, TArg, ClientOp> opFunc,
            Func<ClientSocket, TArg, Task<T>> func,
            PreferredNode preferredNode = default,
            IRetryPolicy? retryPolicyOverride = null)
        {
            var attempt = 0;
            List<Exception>? errors = null;

            while (true)
            {
                ClientSocket? socket = null;

                try
                {
                    socket = await GetSocketAsync(preferredNode).ConfigureAwait(false);

                    return await func(socket, arg).ConfigureAwait(false);
                }
                catch (Exception e)
                {
                    // Preferred node connection may not be available, do not use it after first failure.
                    preferredNode = default;

                    MetricsContext? metricsContext =
                        socket?.MetricsContext
                        ?? (e.Data[ExceptionDataEndpoint] as SocketEndpoint)?.MetricsContext
                        ?? (e.InnerException?.Data[ExceptionDataEndpoint] as SocketEndpoint)?.MetricsContext;

                    IRetryPolicy retryPolicy = retryPolicyOverride ?? Configuration.Configuration.RetryPolicy;

                    if (!HandleOpError(e, opFunc(socket, arg), ref attempt, ref errors, retryPolicy, metricsContext))
                    {
                        throw;
                    }
                }
            }
        }

        /// <inheritdoc/>
        [SuppressMessage("Design", "CA1031:Do not catch general exception types", Justification = "Reviewed.")]
        public void Dispose()
        {
            _socketLock.Wait();

            try
            {
                if (_disposed)
                {
                    return;
                }

                _disposed = true;

                foreach (var endpoint in _endpoints)
                {
                    try
                    {
                        endpoint.Socket?.Dispose();
                    }
                    catch (Exception e)
                    {
                        _logger.LogFailedSocketDispose(e);
                    }
                }
            }
            finally
            {
                _socketLock.Release();
            }
        }

        /// <summary>
        /// Gets active connections.
        /// </summary>
        /// <returns>Active connections.</returns>
        public IList<IConnectionInfo> GetConnections()
        {
            var res = new List<IConnectionInfo>(_endpoints.Count);

            foreach (var endpoint in _endpoints)
            {
                if (endpoint.Socket is { IsDisposed: false, ConnectionContext: { } ctx })
                {
                    res.Add(new ConnectionInfo(ctx.ClusterNode, ctx.SslInfo));
                }
            }

            return res;
        }

        /// <inheritdoc/>
        void IClientSocketEventListener.OnAssignmentChanged(long timestamp)
        {
            while (true)
            {
                var oldTimestamp = Interlocked.Read(ref _assignmentTimestamp);
                if (oldTimestamp >= timestamp)
                {
                    return;
                }

                if (Interlocked.CompareExchange(ref _assignmentTimestamp, value: timestamp, comparand: oldTimestamp) == oldTimestamp)
                {
                    return;
                }
            }
        }

        /// <inheritdoc/>
        void IClientSocketEventListener.OnObservableTimestampChanged(long timestamp)
        {
            // Atomically update the observable timestamp to max(newTs, curTs).
            while (true)
            {
                var current = Interlocked.Read(ref _observableTimestamp);
                if (current >= timestamp)
                {
                    return;
                }

                if (Interlocked.CompareExchange(ref _observableTimestamp, timestamp, current) == current)
                {
                    // TODO: Remove
                    _logger.LogObservableTsUpdatedTrace(current);
                    return;
                }
            }
        }

        /// <summary>
        /// Gets active sockets.
        /// </summary>
        /// <returns>Active sockets.</returns>
        internal IEnumerable<ClientSocket> GetSockets()
        {
            var res = new List<ClientSocket>(_endpoints.Count);

            foreach (var endpoint in _endpoints)
            {
                if (endpoint.Socket is { IsDisposed: false })
                {
                    res.Add(endpoint.Socket);
                }
            }

            return res;
        }

        /// <summary>
        /// Gets a socket. Reconnects if necessary.
        /// </summary>
        /// <param name="preferredNode">Preferred node.</param>
        /// <returns>Client socket.</returns>
        [SuppressMessage(
            "Microsoft.Design",
            "CA1031:DoNotCatchGeneralExceptionTypes",
            Justification = "Any connection exception should be handled.")]
        private async ValueTask<ClientSocket> GetSocketAsync(PreferredNode preferredNode = default)
        {
            ThrowIfDisposed();

            // 1. Preferred node connection.
            if (preferredNode.Name != null && _endpointsByName.TryGetValue(preferredNode.Name, out var endpoint))
            {
                try
                {
                    return await ConnectAsync(endpoint).ConfigureAwait(false);
                }
                catch (Exception e)
                {
                    _logger.LogFailedToConnectPreferredNodeDebug(preferredNode.Name, e.Message);
                }
            }

            // 2. Round-robin connection.
            if (GetNextSocketWithoutReconnect() is { } nextSocket)
            {
                return nextSocket;
            }

            // 3. Default connection.
            return await GetNextSocketAsync().ConfigureAwait(false);
        }

        [SuppressMessage(
            "Microsoft.Design",
            "CA1031:DoNotCatchGeneralExceptionTypes",
            Justification = "Secondary connection errors can be ignored.")]
        private async Task ConnectAllSockets()
        {
            if (_endpoints.Count == 1)
            {
                // No secondary connections to establish.
                return;
            }

            while (!_disposed)
            {
                if (_logger.IsEnabled(LogLevel.Debug))
                {
                    _logger.LogTryingToEstablishSecondaryConnectionsDebug(_endpoints.Count);
                }

                int failed = 0;

                foreach (var endpoint in _endpoints)
                {
                    try
                    {
                        await ConnectAsync(endpoint).ConfigureAwait(false);
                    }
                    catch (Exception e)
                    {
                        _logger.LogErrorWhileEstablishingSecondaryConnectionsWarn(e, e.Message);
                        failed++;
                    }
                }

                if (_logger.IsEnabled(LogLevel.Debug))
                {
                    _logger.LogSecondaryConnectionsEstablishedDebug(_endpoints.Count - failed, failed);
                }

                if (Configuration.Configuration.ReconnectInterval <= TimeSpan.Zero)
                {
                    // Interval is zero - periodic reconnect is disabled.
                    return;
                }

                await Task.Delay(Configuration.Configuration.ReconnectInterval).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Throws if disposed.
        /// </summary>
        private void ThrowIfDisposed() => ObjectDisposedException.ThrowIf(_disposed, this);

        /// <summary>
        /// Gets the next connected socket, or connects a new one.
        /// </summary>
        [SuppressMessage("Maintainability", "CA1508:Avoid dead conditional code", Justification = "False positive")]
        private async ValueTask<ClientSocket> GetNextSocketAsync()
        {
            List<Exception>? errors = null;
            var startIdx = unchecked((int) Interlocked.Increment(ref _endPointIndex));

            for (var i = 0; i < _endpoints.Count; i++)
            {
                var idx = Math.Abs(startIdx + i) % _endpoints.Count;
                var endPoint = _endpoints[idx];

                if (endPoint.Socket is { IsDisposed: false })
                {
                    return endPoint.Socket;
                }

                try
                {
                    return await ConnectAsync(endPoint).ConfigureAwait(false);
                }
                catch (IgniteClientConnectionException e) when (e.GetBaseException() is SocketException or IOException)
                {
                    errors ??= new List<Exception>();

                    e.Data[ExceptionDataEndpoint] = endPoint;

                    errors.Add(e);
                }
            }

            throw new AggregateException(
                "Failed to establish Ignite thin client connection, examine inner exceptions for details.", errors!);
        }

        /// <summary>
        /// Gets the next connected socket, without establishing new connections.
        /// </summary>
        private ClientSocket? GetNextSocketWithoutReconnect()
        {
            var startIdx = unchecked((int) Interlocked.Increment(ref _endPointIndex));

            for (var i = 0; i < _endpoints.Count; i++)
            {
                var idx = Math.Abs(startIdx + i) % _endpoints.Count;
                var endPoint = _endpoints[idx];

                if (endPoint.Socket is { IsDisposed: false })
                {
                    return endPoint.Socket;
                }
            }

            return null;
        }

        /// <summary>
        /// Connects to the given endpoint.
        /// </summary>
        private async ValueTask<ClientSocket> ConnectAsync(SocketEndpoint endpoint)
        {
            if (endpoint.Socket?.IsDisposed == false)
            {
                return endpoint.Socket;
            }

            await _socketLock.WaitAsync().ConfigureAwait(false);

            try
            {
                if (endpoint.Socket?.IsDisposed == false)
                {
                    return endpoint.Socket;
                }

                var socket = await ClientSocket.ConnectAsync(endpoint, Configuration, this).ConfigureAwait(false);

                if (_clusterId == null)
                {
                    _clusterId = socket.ConnectionContext.ClusterId;
                }
                else if (!socket.ConnectionContext.ClusterIds.Contains(_clusterId.Value))
                {
                    socket.Dispose();

                    throw new IgniteClientConnectionException(
                        ErrorGroups.Client.ClusterIdMismatch,
                        $"Cluster ID mismatch: expected={_clusterId}, actual={socket.ConnectionContext.ClusterIds.StringJoin()}");
                }

                endpoint.Socket = socket;

                _endpointsByName[socket.ConnectionContext.ClusterNode.Name] = endpoint;

                return socket;
            }
            finally
            {
                _socketLock.Release();
            }
        }

        /// <summary>
        /// Gets the endpoints: all combinations of IP addresses and ports according to configuration.
        /// </summary>
        private IEnumerable<SocketEndpoint> GetIpEndPoints(IgniteClientConfiguration cfg)
        {
            // Metric collection tools expect numbers and strings, don't pass Guid.
            var clientId = ClientId.ToString();

            foreach (var e in Endpoint.GetEndpoints(cfg))
            {
                var host = e.Host;
                Debug.Assert(host != null, "host != null"); // Checked by GetEndpoints.

                foreach (var ip in GetIps(host))
                {
                    yield return new SocketEndpoint(new IPEndPoint(ip, e.Port), host, clientId);
                }
            }
        }

        /// <summary>
        /// Gets IP address list from a given host.
        /// When host is an IP already - parses it. Otherwise, resolves DNS name to IPs.
        /// </summary>
        private IEnumerable<IPAddress> GetIps(string host, bool suppressExceptions = false)
        {
            try
            {
                // GetHostEntry accepts IPs, but TryParse is a more efficient shortcut.
                return IPAddress.TryParse(host, out var ip) ? new[] { ip } : Dns.GetHostEntry(host).AddressList;
            }
            catch (SocketException e)
            {
                _logger.LogFailedToParseHostDebug(e, host, e.Message);

                if (suppressExceptions)
                {
                    return Enumerable.Empty<IPAddress>();
                }

                throw;
            }
        }

        /// <summary>
        /// Gets a value indicating whether a failed operation should be retried.
        /// </summary>
        /// <param name="exception">Exception that caused the operation to fail.</param>
        /// <param name="op">Operation code.</param>
        /// <param name="attempt">Current attempt.</param>
        /// <param name="retryPolicy">Retry policy.</param>
        /// <returns>
        /// <c>true</c> if the operation should be retried on another connection, <c>false</c> otherwise.
        /// </returns>
        private bool ShouldRetry(Exception exception, ClientOp op, int attempt, IRetryPolicy? retryPolicy)
        {
            var e = exception;

            while (e != null && !IsConnectionError(e))
            {
                e = e.InnerException;
            }

            if (e == null)
            {
                // Only retry connection errors.
                return false;
            }

            if (retryPolicy is null or RetryNonePolicy)
            {
                return false;
            }

            var publicOpType = op.ToPublicOperationType();

            if (publicOpType == null)
            {
                // System operation.
                return true;
            }

            var ctx = new RetryPolicyContext(new(Configuration.Configuration), publicOpType.Value, attempt, exception);

            return retryPolicy.ShouldRetry(ctx);

            static bool IsConnectionError(Exception e) =>
                e is SocketException
                    or IOException
                    or IgniteClientConnectionException { Code: ErrorGroups.Client.Connection };
        }

        /// <summary>
        /// Handles operation error.
        /// </summary>
        /// <param name="exception">Error.</param>
        /// <param name="op">Operation code.</param>
        /// <param name="attempt">Current attempt.</param>
        /// <param name="errors">Previous errors.</param>
        /// <param name="retryPolicy">Retry policy.</param>
        /// <param name="metricsContext">Metrics context.</param>
        /// <returns>True if the error was handled, false otherwise.</returns>
        [SuppressMessage("Microsoft.Design", "CA1002:DoNotExposeGenericLists", Justification = "Private.")]
        private bool HandleOpError(
            Exception exception,
            ClientOp op,
            ref int attempt,
            ref List<Exception>? errors,
            IRetryPolicy? retryPolicy,
            MetricsContext? metricsContext)
        {
            if (!ShouldRetry(exception, op, attempt, retryPolicy))
            {
                if (_logger.IsEnabled(LogLevel.Debug))
                {
                    _logger.LogRetryingOperationDebug("Not retrying", (int)op, op, attempt, exception.Message);
                }

                if (errors == null)
                {
                    return false;
                }

                errors.Add(exception);
                var inner = new AggregateException(errors);

                throw new IgniteClientConnectionException(
                    ErrorGroups.Client.Connection,
                    $"Operation {op} failed after {attempt} retries, examine InnerException for details.",
                    inner);
            }

            if (_logger.IsEnabled(LogLevel.Debug))
            {
                _logger.LogRetryingOperationDebug("Retrying", (int)op, op, attempt, exception.Message);
            }

            Metrics.RequestsRetried.Add(1, metricsContext?.Tags ?? Array.Empty<KeyValuePair<string, object?>>());
            Debug.Assert(metricsContext != null, "metricsContext != null");

            if (errors == null)
            {
                errors = new List<Exception> { exception };
            }
            else
            {
                errors.Add(exception);
            }

            attempt++;

            return true;
        }
    }
}
