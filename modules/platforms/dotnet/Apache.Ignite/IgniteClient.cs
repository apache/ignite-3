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

using System.Threading.Tasks;
using Internal;
using Internal.Common;

/// <summary>
/// Ignite client builder.
/// </summary>
public static class IgniteClient
{
    /// <summary>
    /// Starts the client.
    /// </summary>
    /// <param name="configuration">Configuration.</param>
    /// <returns>Started client.</returns>
    public static async Task<IIgniteClient> StartAsync(IgniteClientConfiguration configuration) =>
        await StartInternalAsync(configuration, DnsResolver.Instance).ConfigureAwait(false);

    /// <summary>
    /// Starts the client.
    /// </summary>
    /// <param name="configuration">Configuration.</param>
    /// <param name="dnsResolver">DNS resolver.</param>
    /// <returns>Started client.</returns>
    internal static async Task<IIgniteClient> StartInternalAsync(IgniteClientConfiguration configuration, IDnsResolver dnsResolver)
    {
        IgniteArgumentCheck.NotNull(configuration);

        var apiTaskSource = new TaskCompletionSource<IgniteApiAccessor>();
        var internalConfig = new IgniteClientConfigurationInternal(
            new(configuration), // Defensive copy.
            apiTaskSource.Task,
            dnsResolver);

        var socket = await ClientFailoverSocket.ConnectAsync(internalConfig).ConfigureAwait(false);
        var client = new IgniteClientInternal(socket);

        apiTaskSource.SetResult(new IgniteApiAccessor(client));
        return client;
    }
}
