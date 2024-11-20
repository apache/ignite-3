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

namespace Apache.Ignite;

using System;
using System.Diagnostics.CodeAnalysis;
using System.Threading;
using System.Threading.Tasks;
using Internal.Common;

/// <summary>
/// Ignite client pool. Thread safe.
/// <para>
/// This pool creates up to <see cref="IgniteClientPoolConfiguration.PoolSize"/> Ignite clients and returns them in a round-robin fashion.
/// Ignite clients are thread safe, so there is no rent/return semantics.
/// </para>
/// </summary>
public sealed class IgniteClientPool : IDisposable
{
    private readonly IIgniteClient?[] _clients;

    private readonly SemaphoreSlim _clientsLock = new(1);

    private int _disposed;

    private int _clientIndex;

    /// <summary>
    /// Initializes a new instance of the <see cref="IgniteClientPool"/> class.
    /// </summary>
    /// <param name="configuration">Configuration.</param>
    public IgniteClientPool(IgniteClientPoolConfiguration configuration)
    {
        IgniteArgumentCheck.NotNull(configuration);
        IgniteArgumentCheck.Ensure(configuration.PoolSize > 0, nameof(configuration.PoolSize), "PoolSize > 0");

        Configuration = configuration;
        _clients = new IIgniteClient[configuration.PoolSize];
    }

    /// <summary>
    /// Gets the configuration.
    /// </summary>
    public IgniteClientPoolConfiguration Configuration { get; }

    /// <summary>
    /// Gets a value indicating whether the pool is disposed.
    /// </summary>
    public bool IsDisposed => Interlocked.CompareExchange(ref _disposed, 0, 0) == 1;

    /// <summary>
    /// Gets an Ignite client from the pool. Creates a new one if necessary.
    /// Performs round-robin balancing across pooled instances.
    /// </summary>
    /// <returns>Ignite client.</returns>
    [SuppressMessage("Reliability", "CA2000:Dispose objects before losing scope", Justification = "Pooled.")]
    public async Task<IIgniteClient> GetClientAsync()
    {
        ObjectDisposedException.ThrowIf(IsDisposed, this);

        int index = Interlocked.Increment(ref _clientIndex) % _clients.Length;

        IIgniteClient? client = _clients[index];
        if (client != null)
        {
            return client;
        }

        await _clientsLock.WaitAsync().ConfigureAwait(false);

        try
        {
            client = _clients[index];
            if (client != null)
            {
                return client;
            }

            client = await CreateClientAsync().ConfigureAwait(false);
            _clients[index] = client;

            return client;
        }
        finally
        {
            _clientsLock.Release();
        }
    }

    /// <inheritdoc/>
    public void Dispose()
    {
        if (Interlocked.CompareExchange(ref _disposed, 1, 0) == 1)
        {
            return;
        }

        _clientsLock.Wait();

        foreach (var client in _clients)
        {
            // Dispose is not supposed to throw, so we expect all clients to dispose correctly.
            client?.Dispose();
        }

        _clientsLock.Dispose();
    }

    private async Task<IIgniteClient> CreateClientAsync() =>
        await IgniteClient.StartAsync(Configuration.ClientConfiguration).ConfigureAwait(false);
}
