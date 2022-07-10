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
        public async Task DeleteTestTable()
        {
            await Client.Sql.ExecuteAsync(null, "DROP TABLE TEST");
        }

        [Test]
        public async Task TestSimpleQuery()
        {
            await using IResultSet<IIgniteTuple> resultSet = await Client.Sql.ExecuteAsync(null, "select 1 as num, 'hello' as str", 1);
            var rows = await resultSet.ToListAsync();

            Assert.AreEqual(-1, resultSet.AffectedRows);
            Assert.IsFalse(resultSet.WasApplied);
            Assert.IsTrue(resultSet.HasRowSet);

            Assert.AreEqual(
                "ResultSetMetadata { Columns = { " +
                "ColumnMetadata { Name = NUM, Type = Int32, Precision = 10, Scale = 0, Nullable = False, Origin =  }, " +
                "ColumnMetadata { Name = STR, Type = String, Precision = 5, Scale = -2147483648, Nullable = False, Origin =  } } }",
                resultSet.Metadata?.ToString());

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
    }
}
