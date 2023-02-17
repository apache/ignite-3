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

namespace Apache.Ignite.Tests;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Internal;

/// <summary>
/// A group of fake servers.
/// </summary>
public sealed class FakeServerGroup : IDisposable
{
    private readonly IReadOnlyList<FakeServer> _servers;

    public FakeServerGroup(IReadOnlyList<FakeServer> servers)
    {
        _servers = servers;
    }

    public static FakeServerGroup Create(int count, Func<FakeServer> factory) =>
        new(Enumerable.Range(1, count).Select(_ => factory()).ToList());

    public async Task<IIgniteClient> ConnectClientAsync(IgniteClientConfiguration? cfg = null)
    {
        cfg ??= new IgniteClientConfiguration();

        cfg.Endpoints.Clear();

        foreach (var server in _servers)
        {
            cfg.Endpoints.Add(server.Endpoint);
        }

        return await IgniteClient.StartAsync(cfg);
    }

    public void Dispose()
    {
        foreach (var server in _servers)
        {
            server.Dispose();
        }
    }
}
