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
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Threading.Tasks;

/// <summary>
/// A group of fake servers.
/// </summary>
public sealed class FakeServerGroup : IDisposable
{
    public FakeServerGroup(IReadOnlyList<FakeServer> servers)
    {
        Servers = servers;
    }

    public IReadOnlyList<FakeServer> Servers { get; }

    [SuppressMessage("Design", "CA1044:Properties should not be write only", Justification = "Tests.")]
    public bool DropNewConnections
    {
        set
        {
            foreach (var server in Servers)
            {
                server.DropNewConnections = value;
            }
        }
    }

    public static FakeServerGroup Create(int count) =>
        new(Enumerable.Range(0, count).Select(_ => new FakeServer()).ToList());

    public static FakeServerGroup Create(int count, Func<int, FakeServer> factory) =>
        new(Enumerable.Range(0, count).Select(factory).ToList());

    public async Task<IIgniteClient> ConnectClientAsync(IgniteClientConfiguration? cfg = null)
    {
        cfg ??= new IgniteClientConfiguration();

        cfg.Endpoints.Clear();

        foreach (var server in Servers)
        {
            cfg.Endpoints.Add(server.Endpoint);
        }

        return await IgniteClient.StartAsync(cfg);
    }

    public void Dispose()
    {
        foreach (var server in Servers)
        {
            server.Dispose();
        }
    }

    public void DropExistingConnections()
    {
        foreach (var server in Servers)
        {
            server.DropExistingConnection();
        }
    }
}
