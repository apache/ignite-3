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
    using Common.Table;
    using Ignite.Table;
    using NUnit.Framework;
    using static Common.Table.TestTables;

    /// <summary>
    /// Tests for <see cref="ITables"/>.
    /// </summary>
    public class TablesTests : IgniteTestsBase
    {
        [Test]
        public async Task TestGetTables()
        {
            var tables = (await Client.Tables.GetTablesAsync()).OrderBy(x => x.Name).ToList();
            var tableNames = tables.Select(x => x.QualifiedName.ObjectName).ToArray();

            Assert.GreaterOrEqual(tables.Count, 2);

            CollectionAssert.Contains(tableNames, TableName);
            CollectionAssert.Contains(tableNames, TableAllColumnsName);

            Assert.AreEqual(QualifiedName.DefaultSchemaName, tables[0].QualifiedName.SchemaName);
        }

        [Test]
        public async Task TestGetExistingTable()
        {
            var table = await Client.Tables.GetTableAsync(TableName);

            Assert.IsNotNull(table);
            Assert.AreEqual(TableName, table!.QualifiedName.ObjectName);
            Assert.AreEqual(QualifiedName.DefaultSchemaName, table.QualifiedName.SchemaName);
            Assert.AreEqual(QualifiedName.Parse(TableName), table.QualifiedName);
        }

        [Test]
        public async Task TestGetExistingTableQuoted()
        {
            var table = await Client.Tables.GetTableAsync("\"PUBLIC\".\"TBL1\"");

            Assert.IsNotNull(table);
            Assert.AreEqual(TableName, table!.QualifiedName.ObjectName);
            Assert.AreEqual(QualifiedName.DefaultSchemaName, table.QualifiedName.SchemaName);
            Assert.AreEqual(QualifiedName.Parse(TableName), table.QualifiedName);
        }

        [Test]
        public async Task TestGetTableReturnsActualName()
        {
            var table = await Client.Tables.GetTableAsync("tBl1");

            Assert.IsNotNull(table);
            Assert.AreEqual("PUBLIC.TBL1", table!.Name);
        }

        [Test]
        public async Task TestGetTableByQualifiedNameReturnsActualName()
        {
            var table = await Client.Tables.GetTableAsync(QualifiedName.Of("public", "tbL1"));

            Assert.IsNotNull(table);
            Assert.AreEqual("PUBLIC.TBL1", table!.Name);
            Assert.AreEqual("TBL1", table.QualifiedName.ObjectName);
        }

        [Test]
        public async Task TestGetExistingTableReturnsSameInstanceEveryTime()
        {
            var table = await Client.Tables.GetTableAsync(TableName);
            var table2 = await Client.Tables.GetTableAsync(TableName);
            var table3 = await Client.Tables.GetTableAsync(QualifiedName.Parse(TableName));

            // Tables and views are cached to avoid extra allocations and serializer handler initializations.
            Assert.AreSame(table, table2);
            Assert.AreSame(table, table3);

            Assert.AreSame(table!.RecordBinaryView, table2!.RecordBinaryView);
            Assert.AreSame(table!.RecordBinaryView, table3!.RecordBinaryView);

            Assert.AreSame(table.GetRecordView<Poco>(), table2.GetRecordView<Poco>());
            Assert.AreSame(table.GetRecordView<Poco>(), table3.GetRecordView<Poco>());
        }

        [Test]
        public async Task TestGetNonExistentTableReturnsNull()
        {
            var tableName = Guid.NewGuid().ToString();
            var table = await Client.Tables.GetTableAsync($"\"{tableName}\"");

            Assert.IsNull(table);
        }

        [Test]
        public async Task TestToString()
        {
            _ = await Client.Tables.GetTablesAsync();

            StringAssert.StartsWith("Tables { CachedTables = [ Table { Name = ", Client.Tables.ToString());
            StringAssert.Contains("{ Name = PUBLIC.TBL_STRING, Id = ", Client.Tables.ToString());
        }
    }
}
