// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

namespace Apache.Ignite.Benchmarks;

using System;
using System.Linq;
using System.Threading.Tasks;

internal static class Program
{
    // IMPORTANT: Disable Netty leak detector when using a real Ignite server for benchmarks.
    private static async Task Main()
    {
        Console.WriteLine("PID: " + Environment.ProcessId);
        Console.WriteLine("Press any key to start the test...");
        Console.ReadKey();

        var cfg = new IgniteClientConfiguration(new[] { 10943, 10944, 10945, 10942 }.Select(port => $"localhost:{port}").ToArray());
        var client1 = await IgniteClient.StartAsync(cfg);
        var client2 = await IgniteClient.StartAsync(cfg);
        var client3 = await IgniteClient.StartAsync(cfg);

        while (true)
        {
            await Task.Delay(10);

            await client1.Tables.GetTablesAsync();
            await client2.Tables.GetTablesAsync();
            await client3.Tables.GetTablesAsync();

#pragma warning disable CA5394
            if (Random.Shared.Next(10) == 1)
#pragma warning restore CA5394
            {
                client2.Dispose();
                client3.Dispose();
                client2 = await IgniteClient.StartAsync(cfg);
                client3 = await IgniteClient.StartAsync(cfg);
            }
        }
    }
}
