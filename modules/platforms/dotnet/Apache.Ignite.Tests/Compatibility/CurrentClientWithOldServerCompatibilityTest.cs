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

namespace Apache.Ignite.Tests.Compatibility;

using System.Linq;
using System.Threading.Tasks;
using Internal;
using Internal.Proto;
using NUnit.Framework;
using TestHelpers;

[TestFixture("3.0.0")]
public class CurrentClientWithOldServerCompatibilityTest
{
    private readonly string _serverVersion;

    private TempDir _workDir;

    private JavaServer _javaServer;

    private IIgniteClient _client;

    public CurrentClientWithOldServerCompatibilityTest(string serverVersion) =>
        _serverVersion = serverVersion;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _workDir = new TempDir();
        _javaServer = await JavaServer.StartOldAsync(_serverVersion, _workDir.Path);

        var cfg = new IgniteClientConfiguration($"localhost:{_javaServer.Port}");
        _client = await IgniteClient.StartAsync(cfg);
    }

    [OneTimeTearDown]
    public void OneTimeTearDown()
    {
        _client.Dispose();
        _javaServer.Dispose();
        _workDir.Dispose();
    }

    [Test]
    public void TestProtocolFeatures()
    {
        ClientSocket socket = ((IgniteClientInternal)_client).Socket.GetSockets().First();
        ProtocolBitmaskFeature features = socket.ConnectionContext.Features;

        switch (_serverVersion)
        {
            case "3.0.0":
                Assert.AreEqual(default(ProtocolBitmaskFeature), features);
                break;

            default:
                Assert.Fail($"Unexpected server version: {_serverVersion}");
                break;
        }
    }

    [Test]
    public async Task TestTables()
    {
        var tables = await _client.Tables.GetTablesAsync();
        Assert.AreEqual(1, tables.Count);
    }
}
