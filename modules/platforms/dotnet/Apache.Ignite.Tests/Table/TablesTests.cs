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

namespace Apache.Ignite.Tests.Table
{
    using System;
    using System.Linq;
    using System.Threading.Tasks;
    using Ignite.Table;
    using NUnit.Framework;

    /// <summary>
    /// Tests for <see cref="ITables"/>.
    /// </summary>
    public class TablesTests : IgniteTestsBase
    {
        [Test]
        public async Task TestGetTables()
        {
            var tables = (await Client.Tables.GetTablesAsync()).OrderBy(x => x.Name).ToList();

            Assert.GreaterOrEqual(tables.Count, 2);
            CollectionAssert.Contains(tables.Select(x => x.Name), TableName);
            CollectionAssert.Contains(tables.Select(x => x.Name), TableAllColumnsName);
        }

        [Test]
        public async Task TestGetExistingTable()
        {
            var table = await Client.Tables.GetTableAsync(TableName);

            Assert.IsNotNull(table);
            Assert.AreEqual(TableName, table!.Name);
        }

        [Test]
        public async Task TestGetExistingTableReturnsSameInstanceEveryTime()
        {
            var table = await Client.Tables.GetTableAsync(TableName);
            var table2 = await Client.Tables.GetTableAsync(TableName);

            // Tables and views are cached to avoid extra allocations and serializer handler initializations.
            Assert.AreSame(table, table2);
            Assert.AreSame(table!.RecordBinaryView, table2!.RecordBinaryView);
            Assert.AreSame(table.GetRecordView<Poco>(), table2.GetRecordView<Poco>());
        }

        [Test]
        public async Task TestGetNonExistentTableReturnsNull()
        {
            var table = await Client.Tables.GetTableAsync(Guid.NewGuid().ToString());

            Assert.IsNull(table);
        }

        [Test]
        public async Task TestToString()
        {
            _ = await Client.Tables.GetTablesAsync();

            StringAssert.StartsWith("Tables { CachedTables = [ Table { Name = ", Client.Tables.ToString());
            StringAssert.Contains("{ Name = TBL_STRING, Id = ", Client.Tables.ToString());
        }
    }
}
