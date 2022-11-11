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

namespace Apache.Ignite.Tests.Linq;

using System;
using System.Linq;
using System.Threading.Tasks;
using Ignite.Table;
using NUnit.Framework;
using Table;

/// <summary>
/// Tests LINQ to SQL conversion.
/// <para />
/// Uses <see cref="FakeServer"/> to get the actual SQL sent from the client.
/// </summary>
public class LinqSqlGenerationTests
{
    public IIgniteClient Client { get; set; } = null!;

    public FakeServer Server { get; set; } = null!;

    public ITable Table { get; set; } = null!;

    [Test]
    public void TestSelectOneColumn()
    {
        AssertSql("select _T0.KEY from PUBLIC.tbl1 as _T0", q => q.Select(x => x.Key).ToList());
    }

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        Server = new FakeServer();
        Client = await Server.ConnectClientAsync();
        Table = (await Client.Tables.GetTableAsync(FakeServer.ExistingTableName))!;
    }

    [OneTimeTearDown]
    public void OneTimeTearDown()
    {
        Client.Dispose();
        Server.Dispose();
    }

    private void AssertSql(string expectedSql, Func<IQueryable<Poco>, object?> query)
    {
        Server.LastSql = string.Empty;

        try
        {
            query(Table.GetRecordView<Poco>().AsQueryable());
        }
        catch (Exception)
        {
            // Ignore.
            // Result deserialization may fail because FakeServer returns one column always.
        }

        Assert.AreEqual(expectedSql, Server.LastSql);
    }
}
