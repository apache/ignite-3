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

    /// <summary>
    /// Client socket wrapper with reconnect/failover functionality.
    /// </summary>
    internal sealed class ClientFailoverSocket : IDisposable
    {
        /** Current global endpoint index for Round-robin. */
        private static long _endPointIndex;

        /** Logger. */
        private readonly IIgniteLogger? _logger;

        /** Endpoints with corresponding hosts - from configuration. */
        private readonly IReadOnlyList<SocketEndpoint> _endPoints;

        /** Cluster node unique name to endpoint map. */
        private readonly ConcurrentDictionary<string, SocketEndpoint> _endPointsMap = new();

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

        /// <summary>
        /// Initializes a new instance of the <see cref="ClientFailoverSocket"/> class.
        /// </summary>
        /// <param name="configuration">Client configuration.</param>
        private ClientFailoverSocket(IgniteClientConfiguration configuration)
        {
            if (configuration.Endpoints.Count == 0)
            {
                throw new IgniteClientException(
                    $"Invalid {nameof(IgniteClientConfiguration)}: " +
                    $"{nameof(IgniteClientConfiguration.Endpoints)} is empty. Nowhere to connect.");
            }

            _logger = configuration.Logger.GetLogger(GetType());
            _endPoints = GetIpEndPoints(configuration).ToList();

            Configuration = new(configuration); // Defensive copy.
        }

        /// <summary>
        /// Gets the configuration.
        /// </summary>
        public IgniteClientConfiguration Configuration { get; }

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
            _ = socket.ConnectAllSockets();

            return socket;
        }

        /// <summary>
        /// Performs an in-out operation.
        /// </summary>
        /// <param name="clientOp">Client op code.</param>
        /// <param name="request">Request data.</param>
        /// <returns>Response data.</returns>
        public async Task<PooledBuffer> DoOutInOpAsync(ClientOp clientOp, PooledArrayBufferWriter? request = null)
        {
            var attempt = 0;
            List<Exception>? errors = null;

            while (true)
            {
                try
                {
                    var socket = await GetSocketAsync().ConfigureAwait(false);

                    return await socket.DoOutInOpAsync(clientOp, request).ConfigureAwait(false);
                }
                catch (Exception e)
                {
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

                _socket?.Dispose();
            }
            finally
            {
                _socketLock.Release();
            }
        }

        /// <summary>
        /// Gets the primary socket. Reconnects if necessary.
        /// </summary>
        /// <returns>Client socket.</returns>
        public async ValueTask<ClientSocket> GetSocketAsync()
        {
            await _socketLock.WaitAsync().ConfigureAwait(false);

            try
            {
                ThrowIfDisposed();

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
            finally
            {
                _socketLock.Release();
            }
        }

        [SuppressMessage(
            "Microsoft.Design",
            "CA1031:DoNotCatchGeneralExceptionTypes",
            Justification = "Secondary connection errors can be ignored.")]
        private async Task ConnectAllSockets()
        {
            if (_endPoints.Count == 1)
            {
                return;
            }

            await _socketLock.WaitAsync().ConfigureAwait(false);

            var tasks = new List<Task>(_endPoints.Count);

            _logger?.Debug("Establishing secondary connections...");

            try
            {
                foreach (var endpoint in _endPoints)
                {
                    if (endpoint.Socket?.IsDisposed == false)
                    {
                        continue;
                    }

                    tasks.Add(ConnectAsync(endpoint));
                }

                await Task.WhenAll(tasks).ConfigureAwait(false);
            }
            catch (Exception e)
            {
                _logger?.Warn("Error while trying to establish secondary connections: " + e.Message, e);
            }
            finally
            {
                _socketLock.Release();
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
        /// Gets next connected socket, or connects a new one.
        /// </summary>
        private async ValueTask<ClientSocket> GetNextSocketAsync()
        {
            List<Exception>? errors = null;
            var startIdx = (int) Interlocked.Increment(ref _endPointIndex);

            for (var i = 0; i < _endPoints.Count; i++)
            {
                var idx = (startIdx + i) % _endPoints.Count;
                var endPoint = _endPoints[idx];

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
                "Failed to establish Ignite thin client connection, examine inner exceptions for details.", errors);
        }

        /// <summary>
        /// Connects to the given endpoint.
        /// </summary>
        private async Task<ClientSocket> ConnectAsync(SocketEndpoint endPoint)
        {
            var socket = await ClientSocket.ConnectAsync(endPoint.EndPoint, Configuration).ConfigureAwait(false);

            endPoint.Socket = socket;

            _endPointsMap[socket.ConnectionContext.ClusterNodeName] = endPoint;

            return socket;
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
                    foreach (var ip in GetIps(e.Host))
                    {
                        yield return new SocketEndpoint(new IPEndPoint(ip, port), e.Host);
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
                IPAddress ip;

                // GetHostEntry accepts IPs, but TryParse is a more efficient shortcut.
                return IPAddress.TryParse(host, out ip) ? new[] {ip} : Dns.GetHostEntry(host).AddressList;
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

                throw new IgniteClientException(
                    $"Operation failed after {attempt} retries, examine InnerException for details.",
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
