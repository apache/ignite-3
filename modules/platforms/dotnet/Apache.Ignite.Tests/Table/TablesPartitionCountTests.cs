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

namespace Apache.Ignite.Tests.Table;

using System.Threading.Tasks;
using NUnit.Framework;

/// <summary>
/// Tests for table partition count changes.
/// </summary>
public class TablesPartitionCountTests : IgniteTestsBase
{
    private const string TestTable = "TEST_TABLE";
    private const string TestZone = "TEST_PARTITION_ZONE";

    [TearDown]
    public async Task DropTestZone()
    {
        var sql = Client.Sql;
        await sql.ExecuteAsync(null, $"DROP TABLE IF EXISTS {TestTable}");
        await sql.ExecuteAsync(null, $"DROP ZONE IF EXISTS {TestZone}");
    }

    [Test]
    public async Task TestTablePartitionCountChange()
    {
        var sql = Client.Sql;

        // Create zone and table with 5 partitions
        await sql.ExecuteAsync(null, $"CREATE ZONE {TestZone} (REPLICAS 1, PARTITIONS 5) STORAGE PROFILES ['default']");
        await sql.ExecuteAsync(null, $"CREATE TABLE {TestTable} (id INT PRIMARY KEY, val VARCHAR) ZONE {TestZone}");

        // Perform operations to ensure client caches partition info
        var table = await Client.Tables.GetTableAsync(TestTable);
        var partitions = await table!.PartitionDistribution.GetPartitionsAsync();
        Assert.AreEqual(5, partitions.Count);

        var view = table.GetKeyValueView<int, string>();
        var key1 = 1;
        await view.PutAsync(null, key1, "value1");

        // Drop and recreate with different partition count
        await sql.ExecuteAsync(null, $"DROP TABLE {TestTable}");
        await sql.ExecuteAsync(null, $"DROP ZONE {TestZone}");
        await sql.ExecuteAsync(null, $"CREATE ZONE {TestZone} (REPLICAS 1, PARTITIONS 10) STORAGE PROFILES ['default']");
        await sql.ExecuteAsync(null, $"CREATE TABLE {TestTable}(id INT PRIMARY KEY, val VARCHAR) ZONE {TestZone}");

        // Old table handle does not work after drop
        Assert.ThrowsAsync<TableNotFoundException>(async () => await view.GetAsync(null, key1));

        // Get the table again
        var table2 = await Client.Tables.GetTableAsync(TestTable);
        var partitions2 = await table2!.PartitionDistribution.GetPartitionsAsync();
        Assert.AreEqual(10, partitions2.Count);

        var view2 = table2.GetKeyValueView<int, string>();
        var result2 = await view2.GetAsync(null, key1);
        Assert.IsFalse(result2.HasValue, "Old key should not exist after table recreation");
    }

    private record Poco(int Id, string Val);
}
