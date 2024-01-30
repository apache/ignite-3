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

namespace Apache.Ignite.Benchmarks;

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Tests;

/// <summary>
/// Establishes many connections to the server node to see how it affects server-side performance.
/// <para />
/// Requires a running Ignite node started with PlatformBenchmarkNodeRunner:
/// <code>gradlew :ignite-runner:runnerPlatformBenchmark</code>.
/// <para />
/// Results on i9-12900H, .NET SDK 6.0.405, Ubuntu 22.04:
///
/// HeartbeatInterval 5 minutes:
/// 250000 connections established in 00:00:41.0241231.
/// 250000 GetTable calls in 00:00:16.2082830.
///
/// | Connections | JVM Used Heap (MB) |
/// | ----------- | ------------------ |
/// | 0           | 155                |
/// | 10000       | 185                |
/// | 20000       | 212                |
/// | 40000       | 265                |
/// | 80000       | 372                |
/// | 160000      | 586                |.
///
/// HeartbeatInterval 1 second:
/// 250000 connections established in 00:01:09.8284032.
/// 250000 GetTable calls in 00:00:51.2001008.
/// </summary>
public static class ManyConnectionsBenchmark
{
    private const int Connections = 250_000;

    public static async Task RunAsync()
    {
        var cfg = new IgniteClientConfiguration
        {
            RetryPolicy = new RetryNonePolicy(),
            LoggerFactory = TestUtils.GetConsoleLoggerFactory(LogLevel.Warning),
            HeartbeatInterval = TimeSpan.FromMinutes(5)
        };

        var clients = new List<IIgniteClient>();

        Console.WriteLine("Establishing connections...");
        var sw = Stopwatch.StartNew();
        TimeSpan lastElapsed = default;

        for (int i = 0; i < Connections; i++)
        {
            // Use different loopback addresses to avoid running out of ports.
            var addr = $"127.0.0.{i % 255}:10420";

            cfg.Endpoints.Clear();
            cfg.Endpoints.Add(addr);

            clients.Add(await IgniteClient.StartAsync(cfg));

            if (sw.Elapsed - lastElapsed > TimeSpan.FromSeconds(2))
            {
                lastElapsed = sw.Elapsed;
                Console.WriteLine($"{i} connections established in {sw.Elapsed}.");
            }
        }

        Console.WriteLine($"{Connections} connections established in {sw.Elapsed}.");
        Console.WriteLine("Invoking GetTable...");
        sw = Stopwatch.StartNew();

        foreach (var client in clients)
        {
            await client.Tables.GetTableAsync("t");
        }

        Console.WriteLine($"{Connections} GetTable calls in {sw.Elapsed}.");
        Console.WriteLine("Press any key to close connections...");
        Console.ReadKey();
    }
}
