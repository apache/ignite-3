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
using System.IO;
using System.Threading.Tasks;
using Internal;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Console;
using NUnit.Framework;

/// <summary>
/// Logging tests.
/// </summary>
public class LoggingTests
{
    [TearDown]
    public void TearDown() => TestUtils.CheckByteArrayPoolLeak(5000);

    [Test]
    [SuppressMessage("ReSharper", "AccessToDisposedClosure", Justification = "Reviewed.")]
    public async Task TestBasicLogging()
    {
        var logger = new ListLoggerFactory(Enum.GetValues<LogLevel>());
        var cfg = new IgniteClientConfiguration
        {
            LoggerFactory = logger,
            SocketTimeout = TimeSpan.FromSeconds(1)
        };

        using (var servers = FakeServerGroup.Create(3))
        using (var client = await servers.ConnectClientAsync(cfg))
        {
            client.WaitForConnections(3);

            await client.Tables.GetTablesAsync();
            await using var cursor = await client.Sql.ExecuteAsync(null, "select 1");
        }

        var log = logger.GetLogString();

        StringAssert.Contains(
            $"Apache.Ignite.Internal.ClientFailoverSocket [Information] " +
            $"Ignite.NET client version {VersionUtils.InformationalVersion} is starting",
            log);

        StringAssert.Contains("[Debug] Connection established", log);
        StringAssert.Contains("[Debug] Handshake succeeded [remoteAddress=[", log);
        StringAssert.Contains("[Trace] Sending request [requestId=1, op=TablesGet, remoteAddress=", log);
        StringAssert.Contains("[Trace] Received response [requestId=1, op=TablesGet, flags=PartitionAssignmentChanged, remoteAddress=", log);
        StringAssert.Contains("op=SqlExec", log);
        StringAssert.Contains("[Debug] Connection closed gracefully", log);
        StringAssert.DoesNotContain("[Error]", log);
    }

    [Test]
    public async Task TestMicrosoftConsoleLogger()
    {
        var oldTextWriter = Console.Out;
        var stringWriter = new StringWriter();
        var textWriter = TextWriter.Synchronized(stringWriter);
        Console.SetOut(textWriter);

        try
        {
            var cfg = new IgniteClientConfiguration
            {
                LoggerFactory = LoggerFactory.Create(builder =>
                {
                    builder.AddConsole(opt =>
                        {
                            opt.MaxQueueLength = 1;
                            opt.QueueFullMode = ConsoleLoggerQueueFullMode.Wait;
                            opt.FormatterName = ConsoleFormatterNames.Simple;
                        })
                        .SetMinimumLevel(LogLevel.Trace);

                    builder.Services.Configure((SimpleConsoleFormatterOptions opts) => opts.ColorBehavior = LoggerColorBehavior.Disabled);
                })
            };

            using var server = new FakeServer();
            using var client = await server.ConnectClientAsync(cfg);
            await client.Tables.GetTablesAsync();
        }
        finally
        {
            Console.SetOut(oldTextWriter);
        }

        // Prevent further writes before accessing the inner StringBuilder.
        textWriter.Close();
        stringWriter.Close();
        var log = stringWriter.ToString();

        StringAssert.Contains("dbug: Apache.Ignite.Internal.ClientSocket", log);
        StringAssert.Contains("Connection established", log);
        StringAssert.Contains("Handshake succeeded [remoteAddress=[", log);
    }
}
