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

namespace Apache.Ignite.Tests.Sql
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;
    using Ignite.Sql;
    using Ignite.Table;
    using NUnit.Framework;

    /// <summary>
    /// Tests for SQL API: <see cref="ISql"/>.
    /// </summary>
    public class SqlTests : IgniteTestsBase
    {
        [OneTimeSetUp]
        public async Task CreateTestTable()
        {
            var createRes = await Client.Sql.ExecuteAsync(null, "CREATE TABLE IF NOT EXISTS TEST(ID INT PRIMARY KEY, VAL VARCHAR)");

            if (!createRes.WasApplied)
            {
                await Client.Sql.ExecuteAsync(null, "DELETE FROM TEST");
            }

            for (var i = 0; i < 10; i++)
            {
                await Client.Sql.ExecuteAsync(null, "INSERT INTO TEST VALUES (?, ?)", i, "s-" + i);
            }
        }

        [OneTimeTearDown]
        public async Task DeleteTestTables()
        {
            await Client.Sql.ExecuteAsync(null, "DROP TABLE TEST");
            await Client.Sql.ExecuteAsync(null, "DROP TABLE IF EXISTS TestDdlDml");
        }

        [Test]
        public async Task TestSimpleQuery()
        {
            await using IResultSet<IIgniteTuple> resultSet = await Client.Sql.ExecuteAsync(null, "select 1 as num, 'hello' as str", 1);
            var rows = await resultSet.ToListAsync();

            Assert.AreEqual(-1, resultSet.AffectedRows);
            Assert.IsFalse(resultSet.WasApplied);
            Assert.IsTrue(resultSet.HasRowSet);
            Assert.IsNotNull(resultSet.Metadata);

            Assert.AreEqual("NUM", resultSet.Metadata!.Columns[0].Name);
            Assert.AreEqual("STR", resultSet.Metadata!.Columns[1].Name);

            Assert.AreEqual(0, resultSet.Metadata!.IndexOf("NUM"));
            Assert.AreEqual(1, resultSet.Metadata!.IndexOf("STR"));

            Assert.AreEqual(1, rows.Count);
            Assert.AreEqual("IgniteTuple { NUM = 1, STR = hello }", rows[0].ToString());
        }

        [Test]
        public async Task TestGetAllMultiplePages()
        {
            var statement = new SqlStatement("SELECT ID, VAL FROM TEST ORDER BY VAL", pageSize: 4);
            await using var resultSet = await Client.Sql.ExecuteAsync(null, statement);
            var rows = await resultSet.ToListAsync();

            Assert.AreEqual(10, rows.Count);
            Assert.AreEqual("IgniteTuple { ID = 0, VAL = s-0 }", rows[0].ToString());
            Assert.AreEqual("IgniteTuple { ID = 9, VAL = s-9 }", rows[9].ToString());
        }

        [Test]
        public async Task TestEnumerateMultiplePages()
        {
            var statement = new SqlStatement("SELECT ID, VAL FROM TEST ORDER BY VAL", pageSize: 4);
            await using var resultSet = await Client.Sql.ExecuteAsync(null, statement);

            var i = 0;

            await foreach (var row in resultSet)
            {
                Assert.AreEqual(i, row["ID"]);
                Assert.AreEqual("s-" + i, row["VAL"]);
                i++;
            }

            Assert.AreEqual(10, i);
        }

        [Test]
        public async Task TestLinqAsyncMultiplePages()
        {
            var statement = new SqlStatement("SELECT ID, VAL FROM TEST ORDER BY VAL", pageSize: 4);
            await using var resultSet = await Client.Sql.ExecuteAsync(null, statement);

            var rows = await resultSet.Where(x => (int)x["ID"]! < 5).ToArrayAsync();
            Assert.AreEqual(5, rows.Length);
            Assert.AreEqual("IgniteTuple { ID = 0, VAL = s-0 }", rows[0].ToString());
            Assert.AreEqual("IgniteTuple { ID = 4, VAL = s-4 }", rows[4].ToString());
        }

        [Test]
        public async Task TestMultipleEnumerationThrows()
        {
            // GetAll -> GetAsyncEnumerator.
            await using var resultSet = await Client.Sql.ExecuteAsync(null, "SELECT 1", 1);
            await resultSet.ToListAsync();

            var ex = Assert.Throws<IgniteClientException>(() => resultSet.GetAsyncEnumerator());
            Assert.AreEqual("Query result set can not be iterated more than once.", ex!.Message);
            Assert.ThrowsAsync<IgniteClientException>(async () => await resultSet.ToListAsync());

            // GetAsyncEnumerator -> GetAll.
            await using var resultSet2 = await Client.Sql.ExecuteAsync(null, "SELECT 1", 1);
            _ = resultSet2.GetAsyncEnumerator();

            Assert.ThrowsAsync<IgniteClientException>(async () => await resultSet2.ToListAsync());
            Assert.Throws<IgniteClientException>(() => resultSet2.GetAsyncEnumerator());
        }

        [Test]
        public async Task TestIterateAfterDisposeThrows()
        {
            var statement = new SqlStatement("SELECT ID, VAL FROM TEST ORDER BY VAL", pageSize: 1);
            var resultSet = await Client.Sql.ExecuteAsync(null, statement);
            var resultSet2 = await Client.Sql.ExecuteAsync(null, statement);

            await resultSet.DisposeAsync();
            await resultSet2.DisposeAsync();

            Assert.ThrowsAsync<ObjectDisposedException>(async () => await resultSet.ToListAsync());

            var enumerator = resultSet2.GetAsyncEnumerator();
            await enumerator.MoveNextAsync(); // Skip first element.
            Assert.ThrowsAsync<ObjectDisposedException>(async () => await enumerator.MoveNextAsync());
        }

        [Test]
        public async Task TestMultipleDisposeIsAllowed()
        {
            var statement = new SqlStatement("SELECT ID, VAL FROM TEST ORDER BY VAL", pageSize: 1);
            await using var resultSet = await Client.Sql.ExecuteAsync(null, statement);

            resultSet.Dispose();
            await resultSet.DisposeAsync();
        }

        [Test]
        public async Task TestPutSqlGetKv()
        {
            var table = await Client.Tables.GetTableAsync("PUBLIC.TEST");
            var res = await table!.RecordBinaryView.GetAsync(null, new IgniteTuple { ["ID"] = 1 });

            Assert.AreEqual("s-1", res!["VAL"]);
        }

        [Test]
        public async Task TestDdlDml()
        {
            // Create table.
            await using var createRes = await Client.Sql.ExecuteAsync(null, "CREATE TABLE TestDdlDml(ID INT PRIMARY KEY, VAL VARCHAR)");

            Assert.IsTrue(createRes.WasApplied);
            Assert.AreEqual(-1, createRes.AffectedRows);
            Assert.IsFalse(createRes.HasRowSet);
            Assert.Throws<IgniteClientException>(() => createRes.GetAsyncEnumerator());
            Assert.ThrowsAsync<IgniteClientException>(async () => await createRes.ToListAsync());
            Assert.IsNull(createRes.Metadata);

            // Insert data.
            for (var i = 0; i < 10; i++)
            {
                var insertRes = await Client.Sql.ExecuteAsync(null, "INSERT INTO TestDdlDml VALUES (?, ?)", i, "hello " + i);

                Assert.IsFalse(insertRes.HasRowSet);
                Assert.IsFalse(insertRes.WasApplied);
                Assert.IsNull(insertRes.Metadata);
                Assert.AreEqual(1, insertRes.AffectedRows);
            }

            // Query data.
            var selectRes = await Client.Sql.ExecuteAsync(null, "SELECT VAL as MYVALUE, ID, ID + 1 FROM TestDdlDml ORDER BY ID");

            Assert.IsTrue(selectRes.HasRowSet);
            Assert.IsFalse(selectRes.WasApplied);
            Assert.AreEqual(-1, selectRes.AffectedRows);

            var columns = selectRes.Metadata!.Columns;
            Assert.AreEqual(3, columns.Count);

            Assert.AreEqual("MYVALUE", columns[0].Name);
            Assert.AreEqual("VAL", columns[0].Origin!.ColumnName);
            Assert.AreEqual("PUBLIC", columns[0].Origin!.SchemaName);
            Assert.AreEqual("TESTDDLDML", columns[0].Origin!.TableName);
            Assert.IsTrue(columns[0].Nullable);
            Assert.AreEqual(SqlColumnType.String, columns[0].Type);

            Assert.AreEqual("ID", columns[1].Name);
            Assert.AreEqual("ID", columns[1].Origin!.ColumnName);
            Assert.AreEqual("PUBLIC", columns[1].Origin!.SchemaName);
            Assert.AreEqual("TESTDDLDML", columns[1].Origin!.TableName);
            Assert.IsFalse(columns[1].Nullable);

            Assert.AreEqual("ID + 1", columns[2].Name);
            Assert.IsNull(columns[2].Origin);

            // Update data.
            var updateRes = await Client.Sql.ExecuteAsync(null, "UPDATE TESTDDLDML SET VAL='upd' WHERE ID < 5");

            Assert.IsFalse(updateRes.WasApplied);
            Assert.IsFalse(updateRes.HasRowSet);
            Assert.IsNull(updateRes.Metadata);
            Assert.AreEqual(5, updateRes.AffectedRows);

            // Drop table.
            var deleteRes = await Client.Sql.ExecuteAsync(null, "DROP TABLE TESTDDLDML");

            Assert.IsFalse(deleteRes.HasRowSet);
            Assert.IsNull(deleteRes.Metadata);
            Assert.IsTrue(deleteRes.WasApplied);
        }

        [Test]
        public void TestInvalidSqlThrowsException()
        {
            var ex = Assert.ThrowsAsync<IgniteClientException>(async () => await Client.Sql.ExecuteAsync(null, "select x from bad"));
            StringAssert.Contains("From line 1, column 15 to line 1, column 17: Object 'BAD' not found", ex!.Message);
        }

        [Test]
        public async Task TestStatementProperties()
        {
            using var server = new FakeServer();
            using var client = await server.ConnectClientAsync();

            var sqlStatement = new SqlStatement(
                query: "SELECT PROPS",
                timeout: TimeSpan.FromSeconds(123),
                schema: "schema-1",
                pageSize: 987,
                properties: new Dictionary<string, object?> { { "prop1", 10 }, { "prop-2", "xyz" } });

            var res = await client.Sql.ExecuteAsync(null, sqlStatement);
            var rows = await res.ToListAsync();
            var props = rows.ToDictionary(x => (string)x["NAME"]!, x => (string)x["VAL"]!);

            Assert.IsTrue(res.HasRowSet);
            Assert.AreEqual(8, props.Count);

            Assert.AreEqual("schema-1", props["schema"]);
            Assert.AreEqual("987", props["pageSize"]);
            Assert.AreEqual("123000", props["timeoutMs"]);
            Assert.AreEqual("SELECT PROPS", props["sql"]);
            Assert.AreEqual("10", props["prop1"]);
            Assert.AreEqual("xyz", props["prop-2"]);
        }
    }
}
