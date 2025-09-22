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

namespace Apache.Ignite.Tests.Sql;

using System.Data;
using System.Linq;
using System.Threading.Tasks;
using Ignite.Sql;
using NUnit.Framework;

public class IgniteDbConnectionTests : IgniteTestsBase
{
    [Test]
    public async Task TestOpenClose()
    {
        var connectionString = $"Endpoints={GetConfig().Endpoints.First()}";
        await using var conn = new IgniteDbConnection(connectionString);
        Assert.AreEqual(ConnectionState.Closed, conn.State);

        await conn.OpenAsync();
        Assert.AreEqual(ConnectionState.Open, conn.State);
        Assert.AreEqual("3.x", conn.ServerVersion);
        Assert.AreEqual(string.Empty, conn.DataSource);
        Assert.AreEqual(string.Empty, conn.Database);
        Assert.AreEqual(connectionString, conn.ConnectionString);
        Assert.IsNotNull(conn.Client);

        await conn.CloseAsync();
        Assert.AreEqual(ConnectionState.Closed, conn.State);
    }

    [Test]
    public async Task TestExistingClient([Values(true, false)] bool ownsClient)
    {
        using var client = await IgniteClient.StartAsync(GetConfig());

        await using var conn = new IgniteDbConnection(null);
        Assert.AreEqual(ConnectionState.Closed, conn.State);

        conn.Open(client, ownsClient);
        Assert.AreEqual(ConnectionState.Open, conn.State);

        await conn.CloseAsync();
        Assert.AreEqual(ConnectionState.Closed, conn.State);

        if (ownsClient)
        {
            Assert.That(client.GetConnections(), Is.Empty, "Client should be closed after connection is closed with ownsClient: true");
        }
        else
        {
            Assert.That(client.GetConnections(), Is.Not.Empty, "Client should be open after connection is closed with ownsClient: false");
        }
    }
}
