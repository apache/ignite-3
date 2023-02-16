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
    using System.Linq;
    using System.Net;
    using System.Net.Sockets;
    using System.Threading;
    using System.Threading.Tasks;
    using Buffers;
    using Log;
    using Proto;
    using Transactions;

    /// <summary>
    /// Client socket wrapper with reconnect/failover functionality.
    /// </summary>
    internal sealed class ClientFailoverSocket : IDisposable
    {
        /** Current global endpoint index for Round-robin. */
        private static long _globalEndPointIndex;

        /** Logger. */
        private readonly IIgniteLogger? _logger;

        /** Endpoints with corresponding hosts - from configuration. */
        private readonly IReadOnlyList<SocketEndpoint> _endpoints;

        /** Cluster node unique name to endpoint map. */
        private readonly ConcurrentDictionary<string, SocketEndpoint> _endpointsByName = new();

        /** Cluster node id to endpoint map. */
        private readonly ConcurrentDictionary<string, SocketEndpoint> _endpointsById = new();

        /** <see cref="_socket"/> lock. */
        [SuppressMessage(
            "Microsoft.Design",
            "CA2213:DisposableFieldsShouldBeDisposed",
            Justification = "WaitHandle is not used in SemaphoreSlim, no need to dispose.")]
        private readonly SemaphoreSlim _socketLock = new(1);

        /** Primary socket. Guarded by <see cref="_socketLock"/>. */
        private ClientSocket? _socket;

        /** Disposed flag. */
        private volatile bool _disposed;

        /** Local topology assignment version. Instead of using event handlers to notify all tables about assignment change,
         * the table will compare its version with channel version to detect an update. */
        private int _assignmentVersion;

        /** Cluster id from the first handshake. */
        private Guid? _clusterId;

        /** Local index for round-robin balancing within this FailoverSocket. */
        private long _endPointIndex = Interlocked.Increment(ref _globalEndPointIndex);

        /// <summary>
        /// Initializes a new instance of the <see cref="ClientFailoverSocket"/> class.
        /// </summary>
        /// <param name="configuration">Client configuration.</param>
        private ClientFailoverSocket(IgniteClientConfiguration configuration)
        {
            if (configuration.Endpoints.Count == 0)
            {
                throw new IgniteClientException(
                    ErrorGroups.Client.Configuration,
                    $"Invalid {nameof(IgniteClientConfiguration)}: {nameof(IgniteClientConfiguration.Endpoints)} is empty. Nowhere to connect.");
            }

            _logger = configuration.Logger.GetLogger(GetType());
            _endpoints = GetIpEndPoints(configuration).ToList();

            Configuration = new(configuration); // Defensive copy.
        }

        /// <summary>
        /// Gets the configuration.
        /// </summary>
        public IgniteClientConfiguration Configuration { get; }

        /// <summary>
        /// Gets the partition assignment version.
        /// </summary>
        public int PartitionAssignmentVersion => Interlocked.CompareExchange(ref _assignmentVersion, -1, -1);

        /// <summary>
        /// Connects the socket.
        /// </summary>
        /// <param name="configuration">Client configuration.</param>
        /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
        public static async Task<ClientFailoverSocket> ConnectAsync(IgniteClientConfiguration configuration)
        {
            var socket = new ClientFailoverSocket(configuration);

            await socket.GetSocketAsync().ConfigureAwait(false);

            // Because this call is not awaited, execution of the current method continues before the call is completed.
            // Secondary connections are established in the background.
            // TODO IGNITE-18808 Do this periodically.
            _ = socket.ConnectAllSockets();

            return socket;
        }

        /// <summary>
        /// Performs an in-out operation.
        /// </summary>
        /// <param name="clientOp">Client op code.</param>
        /// <param name="request">Request data.</param>
        /// <param name="preferredNode">Preferred node.</param>
        /// <returns>Response data and socket.</returns>
        public async Task<PooledBuffer> DoOutInOpAsync(
            ClientOp clientOp,
            PooledArrayBuffer? request = null,
            PreferredNode preferredNode = default)
        {
            var (buffer, _) = await DoOutInOpAndGetSocketAsync(clientOp, tx: null, request, preferredNode).ConfigureAwait(false);

            return buffer;
        }

        /// <summary>
        /// Performs an in-out operation.
        /// </summary>
        /// <param name="clientOp">Client op code.</param>
        /// <param name="tx">Transaction.</param>
        /// <param name="request">Request data.</param>
        /// <param name="preferredNode">Preferred node.</param>
        /// <returns>Response data and socket.</returns>
        public async Task<(PooledBuffer Buffer, ClientSocket Socket)> DoOutInOpAndGetSocketAsync(
            ClientOp clientOp,
            Transaction? tx = null,
            PooledArrayBuffer? request = null,
            PreferredNode preferredNode = default)
        {
            if (tx != null)
            {
                if (tx.FailoverSocket != this)
                {
                    throw new IgniteClientException(ErrorGroups.Client.Connection, "Specified transaction belongs to a different IgniteClient instance.");
                }

                // Use tx-specific socket without retry and failover.
                var buffer = await tx.Socket.DoOutInOpAsync(clientOp, request).ConfigureAwait(false);
                return (buffer, tx.Socket);
            }

            var attempt = 0;
            List<Exception>? errors = null;

            while (true)
            {
                try
                {
                    var socket = await GetSocketAsync(preferredNode).ConfigureAwait(false);

                    var buffer = await socket.DoOutInOpAsync(clientOp, request).ConfigureAwait(false);

                    return (buffer, socket);
                }
                catch (Exception e)
                {
                    // Preferred node connection may not be available, do not use it after first failure.
                    preferredNode = default;

                    if (!HandleOpError(e, clientOp, ref attempt, ref errors))
                    {
                        throw;
                    }
                }
            }
        }

        /// <inheritdoc/>
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
                    endpoint.Socket?.Dispose();
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
        public IEnumerable<ConnectionContext> GetConnections() =>
            _endpoints
                .Select(e => e.Socket?.ConnectionContext)
                .Where(ctx => ctx != null)
                .ToList()!;

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
            if (preferredNode != default)
            {
                var key = preferredNode.Id ?? preferredNode.Name;
                var map = preferredNode.Id != null ? _endpointsById : _endpointsByName;

                if (map.TryGetValue(key!, out var endpoint))
                {
                    try
                    {
                        return await ConnectAsync(endpoint).ConfigureAwait(false);
                    }
                    catch (Exception e)
                    {
                        _logger?.Warn(e, $"Failed to connect to preferred node {preferredNode}: {e.Message}");
                    }
                }
            }

            // 2. Round-robin connection.
            if (GetNextSocketWithoutReconnect() is { } nextSocket)
            {
                return nextSocket;
            }

            // 3. Default connection.
            if (_socket == null || _socket.IsDisposed)
            {
                if (_socket?.IsDisposed == true)
                {
                    _logger?.Info("Primary socket connection lost, reconnecting.");
                }

                _socket = await GetNextSocketAsync().ConfigureAwait(false);
            }

            return _socket;
        }

        [SuppressMessage(
            "Microsoft.Design",
            "CA1031:DoNotCatchGeneralExceptionTypes",
            Justification = "Secondary connection errors can be ignored.")]
        private async Task ConnectAllSockets()
        {
            if (_endpoints.Count == 1)
            {
                return;
            }

            try
            {
                var tasks = new List<Task>(_endpoints.Count);

                _logger?.Debug("Establishing secondary connections...");

                foreach (var endpoint in _endpoints)
                {
                    if (endpoint.Socket?.IsDisposed == false)
                    {
                        continue;
                    }

                    tasks.Add(ConnectAsync(endpoint).AsTask());
                }

                await Task.WhenAll(tasks).ConfigureAwait(false);
            }
            catch (Exception e)
            {
                _logger?.Warn(e, "Error while trying to establish secondary connections: " + e.Message);
            }
        }

        /// <summary>
        /// Throws if disposed.
        /// </summary>
        private void ThrowIfDisposed()
        {
            if (_disposed)
            {
                throw new ObjectDisposedException(nameof(ClientFailoverSocket));
            }
        }

        /// <summary>
        /// Gets the next connected socket, or connects a new one.
        /// </summary>
        [SuppressMessage("Maintainability", "CA1508:Avoid dead conditional code", Justification = "False positive")]
        private async ValueTask<ClientSocket> GetNextSocketAsync()
        {
            List<Exception>? errors = null;
            var startIdx = (int) Interlocked.Increment(ref _endPointIndex);

            for (var i = 0; i < _endpoints.Count; i++)
            {
                var idx = (startIdx + i) % _endpoints.Count;
                var endPoint = _endpoints[idx];

                if (endPoint.Socket is { IsDisposed: false })
                {
                    return endPoint.Socket;
                }

                try
                {
                    return await ConnectAsync(endPoint).ConfigureAwait(false);
                }
                catch (SocketException e)
                {
                    errors ??= new List<Exception>();

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
            var startIdx = (int) Interlocked.Increment(ref _endPointIndex);

            for (var i = 0; i < _endpoints.Count; i++)
            {
                var idx = (startIdx + i) % _endpoints.Count;
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
                var socket = await ClientSocket.ConnectAsync(endpoint.EndPoint, Configuration, OnAssignmentChanged).ConfigureAwait(false);

                if (_clusterId == null)
                {
                    _clusterId = socket.ConnectionContext.ClusterId;
                }
                else if (_clusterId != socket.ConnectionContext.ClusterId)
                {
                    socket.Dispose();

                    throw new IgniteClientConnectionException(
                        ErrorGroups.Client.ClusterIdMismatch,
                        $"Cluster ID mismatch: expected={_clusterId}, actual={socket.ConnectionContext.ClusterId}");
                }

                endpoint.Socket = socket;

                _endpointsByName[socket.ConnectionContext.ClusterNode.Name] = endpoint;
                _endpointsById[socket.ConnectionContext.ClusterNode.Id] = endpoint;

                return socket;
            }
            finally
            {
                _socketLock.Release();
            }
        }

        /// <summary>
        /// Called when an assignment update is detected.
        /// </summary>
        /// <param name="clientSocket">Socket.</param>
        private void OnAssignmentChanged(ClientSocket clientSocket)
        {
            // NOTE: Multiple channels will send the same update to us, resulting in multiple cache invalidations.
            // This could be solved with a cluster-wide AssignmentVersion, but we don't have that.
            // So we only react to updates from the default channel. When no user-initiated operations are performed on the default
            // channel, heartbeat messages will trigger updates.
            if (clientSocket == _socket)
            {
                Interlocked.Increment(ref _assignmentVersion);
            }
        }

        /// <summary>
        /// Gets the endpoints: all combinations of IP addresses and ports according to configuration.
        /// </summary>
        private IEnumerable<SocketEndpoint> GetIpEndPoints(IgniteClientConfiguration cfg)
        {
            foreach (var e in Endpoint.GetEndpoints(cfg))
            {
                var host = e.Host;
                Debug.Assert(host != null, "host != null");  // Checked by GetEndpoints.

                for (var port = e.Port; port <= e.PortRange + e.Port; port++)
                {
                    foreach (var ip in GetIps(host))
                    {
                        yield return new SocketEndpoint(new IPEndPoint(ip, port), host);
                    }
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
                _logger?.Debug(e, "Failed to parse host: " + host);

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
        /// <returns>
        /// <c>true</c> if the operation should be retried on another connection, <c>false</c> otherwise.
        /// </returns>
        private bool ShouldRetry(Exception exception, ClientOp op, int attempt)
        {
            var e = exception;

            while (e != null && !(e is SocketException))
            {
                e = e.InnerException;
            }

            if (e == null)
            {
                // Only retry socket exceptions.
                return false;
            }

            if (Configuration.RetryPolicy is null or RetryNonePolicy)
            {
                return false;
            }

            var publicOpType = op.ToPublicOperationType();

            if (publicOpType == null)
            {
                // System operation.
                return true;
            }

            var ctx = new RetryPolicyContext(new(Configuration), publicOpType.Value, attempt, exception);

            return Configuration.RetryPolicy.ShouldRetry(ctx);
        }

        /// <summary>
        /// Handles operation error.
        /// </summary>
        /// <param name="exception">Error.</param>
        /// <param name="op">Operation code.</param>
        /// <param name="attempt">Current attempt.</param>
        /// <param name="errors">Previous errors.</param>
        /// <returns>True if the error was handled, false otherwise.</returns>
        [SuppressMessage("Microsoft.Design", "CA1002:DoNotExposeGenericLists", Justification = "Private.")]
        private bool HandleOpError(
            Exception exception,
            ClientOp op,
            ref int attempt,
            ref List<Exception>? errors)
        {
            if (!ShouldRetry(exception, op, attempt))
            {
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
