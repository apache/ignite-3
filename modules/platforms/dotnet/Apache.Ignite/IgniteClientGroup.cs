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
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Internal;
using Internal.Common;

/// <summary>
/// Ignite client group. Thread safe.
/// <para>
/// Creates and maintains up to <see cref="IgniteClientGroupConfiguration.Size"/> Ignite clients and returns them in a round-robin fashion.
/// Ignite clients are thread safe, so there is no rent/return semantics.
/// </para>
/// <example>
/// Register as a singleton in DI container:
/// <code>
/// builder.Services.AddSingleton(_ => new IgniteClientGroup(
///     new IgniteClientGroupConfiguration
///     {
///         Size = 3,
///         ClientConfiguration = new("localhost"),
///     }));
/// </code>
/// Invoke from a controller:
/// <code>
/// public async Task&lt;IActionResult&gt; Index([FromServices] IgniteClientGroup igniteGroup)
/// {
///     IIgnite ignite = await igniteGroup.GetIgniteAsync();
///     var tables = await ignite.Tables.GetTablesAsync();
///     return Ok(tables);
/// }
/// </code>
/// </example>
/// </summary>
public sealed class IgniteClientGroup : IDisposable
{
    private readonly IgniteClientGroupConfiguration _configuration;

    private readonly IgniteClientInternal?[] _clients;

    private readonly SemaphoreSlim _clientsLock = new(1);

    /** Shared hybrid timestamp tracker to ensure consistency across client instances. */
    private readonly HybridTimestampTracker _hybridTs = new();

    private int _disposed;

    private int _clientIndex;

    /// <summary>
    /// Initializes a new instance of the <see cref="IgniteClientGroup"/> class.
    /// </summary>
    /// <param name="configuration">Configuration.</param>
    public IgniteClientGroup(IgniteClientGroupConfiguration configuration)
    {
        IgniteArgumentCheck.NotNull(configuration);
        IgniteArgumentCheck.NotNull(configuration.ClientConfiguration);
        IgniteArgumentCheck.Ensure(configuration.Size > 0, nameof(configuration.Size), "Group size must be positive.");

        _configuration = Copy(configuration);
        _clients = new IgniteClientInternal[configuration.Size];
    }

    /// <summary>
    /// Gets the configuration.
    /// </summary>
    public IgniteClientGroupConfiguration Configuration => Copy(_configuration);

    /// <summary>
    /// Gets a value indicating whether the group is disposed.
    /// </summary>
    public bool IsDisposed => Interlocked.CompareExchange(ref _disposed, 0, 0) == 1;

    /// <summary>
    /// Gets an Ignite client from the group. Creates a new one if necessary.
    /// Performs round-robin balancing across grouped instances.
    /// </summary>
    /// <returns>Ignite client.</returns>
    [SuppressMessage("Reliability", "CA2000:Dispose objects before losing scope", Justification = "Managed by the group.")]
    public async ValueTask<IIgnite> GetIgniteAsync()
    {
        ObjectDisposedException.ThrowIf(IsDisposed, this);

        int index = Interlocked.Increment(ref _clientIndex) % _clients.Length;

        IgniteClientInternal? client = _clients[index];
        if (client is { IsDisposed: false })
        {
            return client;
        }

        await _clientsLock.WaitAsync().ConfigureAwait(false);

        try
        {
            client = _clients[index];
            if (client is { IsDisposed: false })
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

    /// <inheritdoc />
    public override string ToString() =>
        new IgniteToStringBuilder(typeof(IgniteClientGroup))
            .Append(_clients.Count(static c => c is { IsDisposed: false }), "Connected")
            .Append(Configuration.Size, "Size")
            .Build();

    private static IgniteClientGroupConfiguration Copy(IgniteClientGroupConfiguration cfg) =>
        cfg with { ClientConfiguration = cfg.ClientConfiguration with { } };

    private async Task<IgniteClientInternal> CreateClientAsync()
    {
        var client = await IgniteClient.StartInternalAsync(
                configuration: Configuration.ClientConfiguration,
                dnsResolver: new DnsResolver(),
                hybridTs: _hybridTs)
            .ConfigureAwait(false);

        return (IgniteClientInternal)client;
    }
}
