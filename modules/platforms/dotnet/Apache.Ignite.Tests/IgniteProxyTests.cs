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

using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Internal.Proto;
using NUnit.Framework;
using static Common.Table.TestTables;

/// <summary>
/// Tests for <see cref="IgniteProxy"/>.
/// </summary>
public class IgniteProxyTests : IgniteTestsBase
{
    [Test]
    public async Task TestBasicProxying()
    {
        EndPoint addr = Client.GetConnections().First().Node.Address;
        using var proxy = new IgniteProxy(addr, "test");
        using var client = await IgniteClient.StartAsync(new IgniteClientConfiguration(proxy.Endpoint));

        var tables = await client.Tables.GetTablesAsync();
        var table = await client.Tables.GetTableAsync(TableName);

        Assert.Greater(tables.Count, 1);
        Assert.IsNotNull(table);

        var ops = proxy.ClientOps.Except([ClientOp.Heartbeat]).ToList();
        Assert.AreEqual(new[] { ClientOp.TablesGetQualified, ClientOp.TableGetQualified }, ops);
    }
}
