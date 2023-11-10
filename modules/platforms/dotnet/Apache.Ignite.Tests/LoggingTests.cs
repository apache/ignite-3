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
using System.Diagnostics.CodeAnalysis;
using System.Threading.Tasks;
using Internal;
using Log;
using NUnit.Framework;

/// <summary>
/// Logging tests.
/// </summary>
public class LoggingTests
{
    [Test]
    [SuppressMessage("ReSharper", "AccessToDisposedClosure", Justification = "Reviewed.")]
    public async Task TestBasicLogging()
    {
        var logger = new ListLogger(new ConsoleLogger { MinLevel = LogLevel.Trace });
        logger.EnabledLevels.Clear();
        logger.EnabledLevels.AddRange(Enum.GetValues<LogLevel>());

        var cfg = new IgniteClientConfiguration { Logger = logger };

        using var servers = FakeServerGroup.Create(3);
        using (var client = await servers.ConnectClientAsync(cfg))
        {
            client.WaitForConnections(3);

            await client.Tables.GetTablesAsync();
            await client.Sql.ExecuteAsync(null, "select 1");
        }

        var log = logger.GetLogString();

        StringAssert.Contains(
            $"ClientFailoverSocket [Info] Ignite.NET client version {VersionUtils.GetInformationalVersion()} is starting",
            log);

        StringAssert.Contains("Connection established", log);
        StringAssert.Contains("Handshake succeeded", log);
        StringAssert.Contains("Trying to establish secondary connections - awaiting 2 tasks", log);
        StringAssert.Contains("2 secondary connections established, 0 failed", log);
        StringAssert.Contains("Sending request [op=TablesGet", log);
        StringAssert.Contains("Sending request [op=SqlExec", log);
        StringAssert.Contains("Connection closed", log);
    }
}
