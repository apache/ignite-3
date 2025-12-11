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

using System.Diagnostics;
using Apache.Ignite;
using Apache.Ignite.Tests.Aot;
using Apache.Ignite.Tests.Aot.Compute;
using Apache.Ignite.Tests.Aot.Sql;
using Apache.Ignite.Tests.Aot.Table;
using Apache.Ignite.Tests.Common;
using Microsoft.Extensions.Logging;

using var javaServer = await JavaServer.StartAsync();

using var logger = new ConsoleLogger(LogLevel.Trace);
var cfg = new IgniteClientConfiguration("localhost:10942")
{
    LoggerFactory = logger
};

using var ignite = await IgniteClient.StartAsync(cfg);
logger.Flush();

var sw = Stopwatch.StartNew();
Console.WriteLine(">>> Starting tests...");

await TestRunner.Run(new TableTests(ignite), logger);
await TestRunner.Run(new ComputeTests(ignite), logger);
await TestRunner.Run(new SqlTests(ignite), logger);

Console.WriteLine($">>> All tests completed in {sw.Elapsed}.");
