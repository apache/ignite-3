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

/// <summary>
/// Establishes many connections to the server node to see how it affects server-side performance.
/// <para />
/// Requires a running Ignite node started with PlatformBenchmarkNodeRunner.
/// </summary>
public static class ManyConnectionsBenchmark
{
    public const int Connections = 10_000;

    public static async Task RunAsync()
    {
        var cfg = new IgniteClientConfiguration("127.0.0.1:10420");
        var clients = new List<IIgniteClient>();

        Console.WriteLine("Establishing connections...");
        var sw = Stopwatch.StartNew();

        for (int i = 0; i < Connections; i++)
        {
            clients.Add(await IgniteClient.StartAsync(cfg));
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

        foreach (var client in clients)
        {
            client.Dispose();
        }
    }
}
