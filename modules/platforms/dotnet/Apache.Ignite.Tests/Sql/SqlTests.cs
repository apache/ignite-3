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
    using System.Diagnostics.CodeAnalysis;
    using System.Globalization;
    using System.Linq;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using Ignite.Sql;
    using Ignite.Table;
    using Ignite.Transactions;
    using Microsoft.Extensions.Logging.Abstractions;
    using NodaTime;
    using NUnit.Framework;

    /// <summary>
    /// Tests for SQL API: <see cref="ISql"/>.
    /// </summary>
    [SuppressMessage("ReSharper", "NotDisposedResource", Justification = "Tests")]
    [SuppressMessage("ReSharper", "NotDisposedResourceIsReturned", Justification = "Tests")]
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
            await Client.Sql.ExecuteAsync(null, "DROP TABLE IF EXISTS TestExecuteScript");
        }

        [SetUp]
        public async Task ResetData()
        {
            await Client.Sql.ExecuteScriptAsync("DELETE FROM TEST WHERE ID >= 10");
        }

        [Test]
        public async Task TestSimpleQuery()
        {
            await using IResultSet<IIgniteTuple> resultSet = await Client.Sql.ExecuteAsync(null, "select 1 as num, 'hello' as str");
            var rows = await resultSet.ToListAsync();

            Assert.AreEqual(-1, resultSet.AffectedRows);
            Assert.IsFalse(resultSet.WasApplied);
            Assert.IsTrue(resultSet.HasRowSet);
            Assert.IsNotNull(resultSet.Metadata);

            Assert.AreEqual("NUM", resultSet.Metadata!.Columns[0].Name);
            Assert.AreEqual("STR", resultSet.Metadata!.Columns[1].Name);

            Assert.AreEqual(0, resultSet.Metadata!.IndexOf("NUM"));
            Assert.AreEqual(1, resultSet.Metadata!.IndexOf("STR"));

            Assert.AreEqual(
                "ResultSetMetadata { Columns = [ " +
                "ColumnMetadata { Name = NUM, Type = Int32, Precision = 10, Scale = 0, Nullable = False, Origin =  }, " +
                "ColumnMetadata { Name = STR, Type = String, Precision = 5, Scale = -2147483648, Nullable = False, Origin =  } ] }",
                resultSet.Metadata.ToString());

            Assert.AreEqual(1, rows.Count);
            Assert.AreEqual("IgniteTuple { NUM = 1, STR = hello }", rows[0].ToString());
        }

        [Test]
        public async Task TestNullArgs()
        {
            await using IResultSet<IIgniteTuple> resultSet = await Client.Sql.ExecuteAsync(
                transaction: null,
                statement: "select 1",
                args: null);

            var rows = await resultSet.ToListAsync();

            Assert.AreEqual(1, rows.Single()[0]);
        }

        [Test]
        public async Task TestNullArg()
        {
            await Client.Sql.ExecuteAsync(null, "insert into TEST (ID, VAL) VALUES (-1, NULL)");

            await using IResultSet<IIgniteTuple> resultSet = await Client.Sql.ExecuteAsync(
                transaction: null,
                statement: "select ID from TEST where VAL = ?",
                args: new object?[] { null });

            var rows = await resultSet.ToListAsync();

            await using IResultSet<IIgniteTuple> resultSet2 = await Client.Sql.ExecuteAsync(
                transaction: null,
                statement: "select ID from TEST where VAL is null");

            var rows2 = await resultSet2.ToListAsync();

            CollectionAssert.IsEmpty(rows);
            Assert.AreEqual(1, rows2.Count);
        }

        [Test]
        public async Task TestGetAllMultiplePages([Values(1, 2, 3, 4, 5, 6)] int pageSize)
        {
            var statement = new SqlStatement("SELECT ID, VAL FROM TEST ORDER BY VAL", pageSize: pageSize);
            await using var resultSet = await Client.Sql.ExecuteAsync(null, statement);
            var rows = await resultSet.ToListAsync();

            Assert.AreEqual(10, rows.Count);
            Assert.AreEqual("IgniteTuple { ID = 0, VAL = s-0 }", rows[0].ToString());
            Assert.AreEqual("IgniteTuple { ID = 9, VAL = s-9 }", rows[9].ToString());
        }

        [Test]
        public async Task TestToDictionary()
        {
            var statement = new SqlStatement("SELECT ID, VAL FROM TEST WHERE VAL IS NOT NULL ORDER BY VAL", pageSize: 2);
            await using var resultSet = await Client.Sql.ExecuteAsync(null, statement);
            Dictionary<int, string> res = await resultSet.ToDictionaryAsync(x => (int)x["ID"]!, x => (string)x["VAL"]!);

            Assert.AreEqual(10, res.Count);
            Assert.AreEqual("s-3", res[3]);
        }

        [Test]
        public async Task TestToDictionaryCustomComparer()
        {
            await using var resultSet = await Client.Sql.ExecuteAsync(null, "SELECT ID, VAL FROM TEST WHERE VAL IS NOT NULL ORDER BY VAL");
            Dictionary<string, int> res = await resultSet.ToDictionaryAsync(
                x => (string)x["VAL"]!,
                x => (int)x["ID"]!,
                StringComparer.OrdinalIgnoreCase);

            Assert.AreEqual(10, res.Count);
            Assert.AreEqual(3, res["s-3"]);
            Assert.AreEqual(3, res["S-3"]);
            Assert.IsFalse(res.ContainsKey("x-3"));
        }

        [Test]
        public async Task TestCollect()
        {
            var statement = new SqlStatement("SELECT ID, VAL FROM TEST WHERE VAL IS NOT NULL ORDER BY VAL", pageSize: 2);
            await using var resultSet = await Client.Sql.ExecuteAsync(null, statement);
            HashSet<int> res = await resultSet.CollectAsync(capacity => new HashSet<int>(capacity), (set, row) => set.Add((int)row["ID"]!));

            Assert.AreEqual(10, res.Count);
            Assert.IsTrue(res.Contains(1));
            Assert.IsFalse(res.Contains(111));
        }

        [Test]
        public async Task TestExists()
        {
            await using var resultSet = await Client.Sql.ExecuteAsync(null, "SELECT EXISTS (SELECT 1 FROM TEST WHERE ID > 1)");
            var rows = await resultSet.ToListAsync();

            Assert.AreEqual(1, rows.Count);
            Assert.AreEqual(true, rows[0][0]);
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
            await using var resultSet = await Client.Sql.ExecuteAsync(null, "SELECT 1");
            await resultSet.ToListAsync();

            var ex = Assert.Throws<IgniteClientException>(() => resultSet.GetAsyncEnumerator());
            Assert.AreEqual("Query result set can not be iterated more than once.", ex!.Message);
            Assert.ThrowsAsync<IgniteClientException>(async () => await resultSet.ToListAsync());

            // GetAsyncEnumerator -> GetAll.
            await using var resultSet2 = await Client.Sql.ExecuteAsync(null, "SELECT 1");
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
            var table = await Client.Tables.GetTableAsync("TEST");
            var res = await table!.RecordBinaryView.GetAsync(null, new IgniteTuple { ["ID"] = 1 });

            Assert.AreEqual("s-1", res.Value["VAL"]);
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
                await using var insertRes = await Client.Sql.ExecuteAsync(null, "INSERT INTO TestDdlDml VALUES (?, ?)", i, "hello " + i);

                Assert.IsFalse(insertRes.HasRowSet);
                Assert.IsFalse(insertRes.WasApplied);
                Assert.IsNull(insertRes.Metadata);
                Assert.AreEqual(1, insertRes.AffectedRows);
            }

            // Query data.
            await using var selectRes = await Client.Sql.ExecuteAsync(null, "SELECT VAL as MYVALUE, ID, ID + 1 FROM TestDdlDml ORDER BY ID");

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
            Assert.AreEqual(ColumnType.String, columns[0].Type);
            Assert.AreEqual(int.MinValue, columns[0].Scale);
            Assert.AreEqual(65536, columns[0].Precision);

            Assert.AreEqual("ID", columns[1].Name);
            Assert.AreEqual("ID", columns[1].Origin!.ColumnName);
            Assert.AreEqual("PUBLIC", columns[1].Origin!.SchemaName);
            Assert.AreEqual("TESTDDLDML", columns[1].Origin!.TableName);
            Assert.IsFalse(columns[1].Nullable);
            Assert.AreEqual(0, columns[1].Scale);
            Assert.AreEqual(10, columns[1].Precision);

            Assert.AreEqual("ID + 1", columns[2].Name);
            Assert.IsNull(columns[2].Origin);

            // Update data.
            await using var updateRes = await Client.Sql.ExecuteAsync(null, "UPDATE TESTDDLDML SET VAL='upd' WHERE ID < 5");

            Assert.IsFalse(updateRes.WasApplied);
            Assert.IsFalse(updateRes.HasRowSet);
            Assert.IsNull(updateRes.Metadata);
            Assert.AreEqual(5, updateRes.AffectedRows);

            // Drop table.
            await using var deleteRes = await Client.Sql.ExecuteAsync(null, "DROP TABLE TESTDDLDML");

            Assert.IsFalse(deleteRes.HasRowSet);
            Assert.IsNull(deleteRes.Metadata);
            Assert.IsTrue(deleteRes.WasApplied);
        }

        [Test]
        public void TestInvalidTableNameInSqlThrowsException()
        {
            var ex = Assert.ThrowsAsync<SqlException>(async () => await Client.Sql.ExecuteAsync(null, "select x from bad"));

            StringAssert.Contains("From line 1, column 15 to line 1, column 17: Object 'BAD' not found", ex!.Message);
        }

        [Test]
        public void TestInvalidSqlThrowsException()
        {
            var ex = Assert.ThrowsAsync<SqlException>(async () => await Client.Sql.ExecuteAsync(null, "foo bar baz"));
            var inner = (SqlException)ex!.InnerException!;

            Assert.AreEqual(
                "Invalid query, check inner exceptions for details: SqlStatement { " +
                "Query = foo bar baz, Timeout = 00:00:00, Schema = PUBLIC, PageSize = 1024, Properties = { } }",
                ex.Message);

            Assert.AreEqual(
                "Failed to parse query: Non-query expression encountered in illegal context. At line 1, column 1",
                inner.Message);
        }

        [Test]
        public void TestCreateTableExistsThrowsException()
        {
            var ex = Assert.ThrowsAsync<SqlException>(
                async () => await Client.Sql.ExecuteAsync(null, "CREATE TABLE TEST(ID INT PRIMARY KEY)"));

            StringAssert.Contains("Table with name 'PUBLIC.TEST' already exists", ex!.Message);
        }

        [Test]
        public void TestAlterTableNotFoundThrowsException()
        {
            var ex = Assert.ThrowsAsync<SqlException>(
                async () => await Client.Sql.ExecuteAsync(null, "ALTER TABLE NOT_EXISTS_TABLE ADD COLUMN VAL1 VARCHAR"));

            StringAssert.Contains("Table with name 'PUBLIC.NOT_EXISTS_TABLE' not found", ex!.Message);
        }

        [Test]
        public void TestAlterTableColumnExistsThrowsException()
        {
            var ex = Assert.ThrowsAsync<SqlException>(
                async () => await Client.Sql.ExecuteAsync(null, "ALTER TABLE TEST ADD COLUMN ID INT"));

            StringAssert.Contains("Failed to validate query. Column with name 'ID' already exists", ex!.Message);
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
                timeZoneId: "Europe/London",
                properties: new Dictionary<string, object?> { { "prop1", 10 }, { "prop-2", "xyz" } });

            await using var res = await client.Sql.ExecuteAsync(null, sqlStatement);
            var rows = await res.ToListAsync();
            var props = rows.ToDictionary(x => (string)x["NAME"]!, x => (string)x["VAL"]!);

            Assert.IsTrue(res.HasRowSet);
            Assert.AreEqual(9, props.Count);

            Assert.AreEqual("schema-1", props["schema"]);
            Assert.AreEqual("987", props["pageSize"]);
            Assert.AreEqual("123000", props["timeoutMs"]);
            Assert.AreEqual("SELECT PROPS", props["sql"]);
            Assert.AreEqual("10", props["prop1"]);
            Assert.AreEqual("xyz", props["prop-2"]);
            Assert.AreEqual("Europe/London", props["timeZoneId"]);
        }

        [Test]
        public async Task TestResultSetToString()
        {
            await using IResultSet<IIgniteTuple> resultSet = await Client.Sql.ExecuteAsync(null, "select 1 as num, 'hello' as str");

            Assert.AreEqual(
                "ResultSet`1[IIgniteTuple] { HasRowSet = True, AffectedRows = -1, WasApplied = False, " +
                "Metadata = ResultSetMetadata { Columns = [ " +
                "ColumnMetadata { Name = NUM, Type = Int32, Precision = 10, Scale = 0, Nullable = False, Origin =  }, " +
                "ColumnMetadata { Name = STR, Type = String, Precision = 5, Scale = -2147483648, Nullable = False, Origin =  } ] } }",
                resultSet.ToString());
        }

        [Test]
        public async Task TestSelectNull()
        {
            await using IResultSet<IIgniteTuple> resultSet = await Client.Sql.ExecuteAsync(null, "select null");

            var rows = await resultSet.ToListAsync();

            Assert.AreEqual(1, rows.Count);
            Assert.IsNull(rows[0][0]);

            Assert.AreEqual(
                "ResultSet`1[IIgniteTuple] { HasRowSet = True, AffectedRows = -1, WasApplied = False, " +
                "Metadata = ResultSetMetadata { Columns = [ " +
                "ColumnMetadata { Name = NULL, Type = Null, Precision = -1, Scale = -2147483648, Nullable = True, Origin =  } ] } }",
                resultSet.ToString());
        }

        [Test]
        public async Task TestExecuteScript()
        {
            var id = Random.Shared.Next(100);

            await Client.Sql.ExecuteScriptAsync(
                "CREATE TABLE TestExecuteScript(ID INT PRIMARY KEY, VAL VARCHAR); " +
                "DELETE FROM TestExecuteScript;" +
                "INSERT INTO TestExecuteScript VALUES (?, ?); " +
                "INSERT INTO TestExecuteScript VALUES (?, ?);",
                CancellationToken.None,
                id,
                "a",
                id + 1,
                "b");

            await using var resultSet = await Client.Sql.ExecuteAsync(null, "SELECT * FROM TESTEXECUTESCRIPT ORDER BY ID");
            var rows = await resultSet.ToListAsync();

            Assert.AreEqual(2, rows.Count);
            Assert.AreEqual($"IgniteTuple {{ ID = {id}, VAL = a }}", rows[0].ToString());
            Assert.AreEqual($"IgniteTuple {{ ID = {id + 1}, VAL = b }}", rows[1].ToString());
        }

        [Test]
        public async Task TestScriptProperties()
        {
            using var server = new FakeServer();
            using var client = await server.ConnectClientAsync();

            var sqlStatement = new SqlStatement(
                query: "SELECT PROPS",
                timeout: TimeSpan.FromSeconds(123),
                schema: "schema-1",
                pageSize: 987,
                properties: new Dictionary<string, object?> { { "prop1", 10 }, { "prop-2", "xyz" } },
                timeZoneId: "Europe/Nicosia");

            await client.Sql.ExecuteScriptAsync(sqlStatement);
            var resProps = server.LastSqlScriptProps;

            Assert.AreEqual("schema-1", resProps["schema"]);
            Assert.AreEqual(987, resProps["pageSize"]);
            Assert.AreEqual(123000, resProps["timeoutMs"]);
            Assert.AreEqual("SELECT PROPS", resProps["sql"]);
            Assert.AreEqual(10, resProps["prop1"]);
            Assert.AreEqual("xyz", resProps["prop-2"]);
            Assert.AreEqual(sqlStatement.TimeZoneId, resProps["timeZoneId"]);
        }

        [Test]
        public void TestInvalidScriptThrowsSqlException()
        {
            var ex = Assert.ThrowsAsync<SqlException>(async () => await Client.Sql.ExecuteScriptAsync("CREATE SOMETHING"));
            var inner = (SqlException)ex!.InnerException!;

            Assert.AreEqual(
                "Invalid query, check inner exceptions for details: SqlStatement { " +
                "Query = CREATE SOMETHING, " +
                "Timeout = 00:00:00, " +
                "Schema = PUBLIC, " +
                "PageSize = 1024, " +
                "Properties = { } }",
                ex.Message);

            Assert.AreEqual("Failed to parse query: Encountered \"SOMETHING\" at line 1, column 8", inner.Message);
        }

        [Test]
        public async Task TestCustomDecimalScale()
        {
            await using var resultSet = await Client.Sql.ExecuteAsync(null, "select cast((10 / ?) as decimal(20, 5))", 3m);
            IIgniteTuple res = await resultSet.SingleAsync();

            var bigDecimal = (BigDecimal)res[0]!;

            Assert.AreEqual(3.33333m, bigDecimal.ToDecimal());
            Assert.AreEqual(5, bigDecimal.Scale);
        }

        [Test]
        public async Task TestMaxDecimalScale()
        {
            await using var resultSet = await Client.Sql.ExecuteAsync(null, "select (10 / ?::decimal)", 3m);
            IIgniteTuple res = await resultSet.SingleAsync();

            var bigDecimal = (BigDecimal)res[0]!;

            Assert.AreEqual(32757, bigDecimal.Scale);
            Assert.AreEqual(13603, bigDecimal.UnscaledValue.GetByteCount());
            StringAssert.StartsWith("3.3333333333", bigDecimal.ToString(CultureInfo.InvariantCulture));
        }

        [Test]
        public async Task TestStatementTimeZoneWithAllZones()
        {
            using var client = await IgniteClient.StartAsync(GetConfig() with { LoggerFactory = NullLoggerFactory.Instance });
            var statement = new SqlStatement("SELECT CURRENT_TIMESTAMP");

            ICollection<string> zoneIds = DateTimeZoneProviders.Tzdb.Ids;
            var zoneProvider = DateTimeZoneProviders.Tzdb;

            List<Exception> failures = new();

            foreach (var zoneId in zoneIds)
            {
                try
                {
                    await using var resultSet = await client.Sql.ExecuteAsync(null, statement with { TimeZoneId = zoneId });

                    var resTime = (Instant)(await resultSet.SingleAsync())[0]!;

                    var currentTimeInZone = SystemClock.Instance.GetCurrentInstant();

                    AssertInstantSimilar(currentTimeInZone, resTime, zoneId);
                }
                catch (Exception e)
                {
                    failures.Add(e);
                    Console.WriteLine("Time zone mismatch between .NET and Java: " + e.Message);
                }
            }

            // JDK, CLR, and NodaTime have time zone databases that are updated at different times, we expect some mismatches.
            if (failures.Count > 30)
            {
                throw new AggregateException("Too many failures: " + failures.Count, failures);
            }

            Console.WriteLine($"{zoneIds.Count - failures.Count} time zones match in .NET and Java.");
        }

        [Test]
        public async Task TestStatementTimezoneAsUtcOffset([Values(0, 5, 10)] int offset)
        {
            var statement = new SqlStatement("SELECT CURRENT_TIMESTAMP", timeZoneId: $"UTC+{offset}");
            await using var resultSet = await Client.Sql.ExecuteAsync(null, statement);
            var resTime = (Instant)(await resultSet.SingleAsync())[0]!;

            var expectedTime = SystemClock.Instance.GetCurrentInstant();

            AssertInstantSimilar(expectedTime, resTime, $"Offset: {offset}");
        }

        [Test]
        public async Task TestExecuteBatch()
        {
            long[] res = await Client.Sql.ExecuteBatchAsync(
                transaction: null,
                statement: "INSERT INTO TEST VALUES (?, ?)",
                args: [[100, "x"], [101, "y"], [102, "z"]]);

            CollectionAssert.AreEqual(new[] { 1L, 1L, 1L }, res);

            await using var resultSet = await Client.Sql.ExecuteAsync(
                null, "SELECT ID, VAL FROM TEST WHERE ID >= 100 AND ID <= 102 ORDER BY ID");

            List<IIgniteTuple> rows = await resultSet.ToListAsync();
            Assert.AreEqual(3, rows.Count);

            Assert.AreEqual("IgniteTuple { ID = 100, VAL = x }", rows[0].ToString());
            Assert.AreEqual("IgniteTuple { ID = 101, VAL = y }", rows[1].ToString());
            Assert.AreEqual("IgniteTuple { ID = 102, VAL = z }", rows[2].ToString());
        }

        [Test]
        public async Task TestExecuteBatchInsertUpdateDelete()
        {
            long[] insertRes = await Client.Sql.ExecuteBatchAsync(
                transaction: null,
                statement: "INSERT INTO TEST VALUES (?, ?)",
                args: [[100, "x"], [101, "y"], [102, "z"]]);

            CollectionAssert.AreEqual(new[] { 1L, 1L, 1L }, insertRes);

            long[] updateRes = await Client.Sql.ExecuteBatchAsync(
                transaction: null,
                statement: "UPDATE TEST SET VAL = ? WHERE ID >= ? AND ID <= ?",
                args: [["update1", 100, 101], ["update2", 102, 103]]);

            CollectionAssert.AreEqual(new[] { 2L, 1L }, updateRes);

            long[] deleteRes = await Client.Sql.ExecuteBatchAsync(
                transaction: null,
                statement: "DELETE FROM TEST WHERE ID >= ? AND ID <= ?",
                args: [[100, 102]]);

            CollectionAssert.AreEqual(new[] { 3L }, deleteRes);
        }

        [Test]
        public async Task TestExecuteBatchArgsCollections()
        {
            var statement = "INSERT INTO TEST VALUES (?, ?)";

            // Array.
            object[][] arr =
            [
                [200, "x"],
                [201, "y"],
            ];

            await Client.Sql.ExecuteBatchAsync(null, statement, arr);

            // List.
            List<List<object>> args =
            [
                [300, "x"],
                [301, "y"]
            ];

            await Client.Sql.ExecuteBatchAsync(null, statement, args);

            // Lazy.
            IEnumerable<IEnumerable<object>> collection = Yield(
                Yield<object>(401, "x1"),
                Yield<object>(402, "x2"));

            await Client.Sql.ExecuteBatchAsync(null, statement, collection);

            static IEnumerable<T> Yield<T>(params T[] args)
            {
                foreach (var arg in args)
                {
                    yield return arg;
                }
            }
        }

        [Test]
        public async Task TestExecuteBatchWithTx()
        {
            await using var tx = await Client.Transactions.BeginAsync();

            Assert.AreEqual(0, await GetCount(tx));

            await Client.Sql.ExecuteBatchAsync(tx, "INSERT INTO TEST VALUES (?, ?)", [[110, "x"], [111, "y"]]);

            Assert.AreEqual(1, await GetCount(tx));

            Assert.AreEqual(0, await GetCount(null));

            async Task<int> GetCount(ITransaction? txn)
            {
                await using var resultSet = await Client.Sql.ExecuteAsync(txn, "SELECT ID, VAL FROM TEST WHERE ID = 110");
                var rows = await resultSet.ToListAsync();
                return rows.Count;
            }
        }

        [Test]
        public async Task TestExecuteBatchNullArg()
        {
            var res = await Client.Sql.ExecuteBatchAsync(null, "DELETE FROM TEST WHERE VAL IS NOT DISTINCT FROM ?", [[null]]);
            Assert.AreEqual(new[] { 0L }, res);
        }

        [Test]
        public void TestExecuteBatchMissingArgs()
        {
            var ex = Assert.ThrowsAsync<ArgumentException>(
                async () => await Client.Sql.ExecuteBatchAsync(null, "select 1", []));

            StringAssert.Contains("Batch arguments must not be empty.", ex.Message);

            var ex2 = Assert.ThrowsAsync<ArgumentException>(
                async () => await Client.Sql.ExecuteBatchAsync(null, "select 1", [[]]));

            StringAssert.Contains("Batch arguments must not contain empty rows.", ex2.Message);
        }

        [Test]
        public void TestExecuteBatchMismatchingArgs()
        {
            var ex = Assert.ThrowsAsync<ArgumentException>(
                async () => await Client.Sql.ExecuteBatchAsync(null, "select 1", [[1], [2, 3]]));

            Assert.AreEqual("Inconsistent batch argument size: Expected 1 objects, but got more.", ex.Message);

            var ex2 = Assert.ThrowsAsync<ArgumentException>(
                async () => await Client.Sql.ExecuteBatchAsync(null, "select 1", [[1, 2], [3]]));

            Assert.AreEqual("Inconsistent batch argument size: Expected 2 objects, but got 1.", ex2.Message);

            var ex3 = Assert.ThrowsAsync<ArgumentException>(
                async () => await Client.Sql.ExecuteBatchAsync(null, "select 1", [[1], []]));

            Assert.AreEqual("Inconsistent batch argument size: Expected 1 objects, but got 0.", ex3.Message);
        }

        [Test]
        public void TestExecuteBatchNullArgRow()
        {
            Assert.ThrowsAsync<ArgumentNullException>(
                async () => await Client.Sql.ExecuteBatchAsync(null, "select 1", [[1], null!, [2]]));
        }

        [Test]
        public void TestExecuteBatchInvalidStatement()
        {
            var ex = Assert.ThrowsAsync<SqlBatchException>(
                async () => await Client.Sql.ExecuteBatchAsync(null, "select 1", [[1]]));

            Assert.AreEqual("Invalid SQL statement type. Expected [DML] but got QUERY.", ex.Message);
        }

        [Test]
        public async Task TestCancelQueryCursor([Values(true, false)] bool beforeIter)
        {
            var cts = new CancellationTokenSource();
            await using var cursor = await Client.Sql.ExecuteAsync(
                transaction: null,
                new SqlStatement("SELECT * FROM TEST") { PageSize = 1 },
                cts.Token);

            if (beforeIter)
            {
                cts.Cancel();
            }

            Assert.ThrowsAsync<OperationCanceledException>(async () =>
            {
                await foreach (var unused in cursor)
                {
                    if (!beforeIter)
                    {
                        cts.Cancel();
                    }
                }
            });

            Assert.IsFalse(TestUtils.HasCallbacks(cts));
        }

        [Test]
        public async Task TestCancelQueryExecute([Values("sql", "sql-mapped", "script", "reader", "batch")] string mode)
        {
            // Cross join will produce 10^N rows, which takes a while to execute.
            var manyRowsQuery = $"select count (*) from ({GenerateCrossJoin(8)})";

            using var cts = new CancellationTokenSource();

            Task task = mode switch
            {
                "sql" => Client.Sql.ExecuteAsync(transaction: null, manyRowsQuery, cts.Token),
                "sql-mapped" => Client.Sql.ExecuteAsync<int>(transaction: null, manyRowsQuery, cts.Token),
                "script" => Client.Sql.ExecuteScriptAsync($"DELETE FROM {TableName} WHERE KEY = ({manyRowsQuery})", cts.Token),
                "reader" => Client.Sql.ExecuteReaderAsync(transaction: null, manyRowsQuery, cts.Token),
                "batch" => Client.Sql.ExecuteBatchAsync(null, $"DELETE FROM {TableName} WHERE KEY = ({manyRowsQuery}) + ?", [[1]], cts.Token),
                _ => throw new ArgumentException("Invalid mode: " + mode)
            };

            // Wait for request to be sent and callbacks to be registered, then cancel.
            TestUtils.WaitForCancellationRegistrations(cts);
            await cts.CancelAsync();

            var ex = Assert.ThrowsAsync<OperationCanceledException>(async () => await task);
            Assert.AreEqual("The query was cancelled while executing.", ex!.Message);

            if (mode == "batch")
            {
                Assert.IsInstanceOf<SqlBatchException>(ex.InnerException);
            }
            else
            {
                Assert.IsInstanceOf<SqlException>(ex.InnerException);
            }

            Assert.IsFalse(TestUtils.HasCallbacks(cts));
        }

        private static string GenerateCrossJoin(int depth)
        {
            var sb = new StringBuilder("SELECT ");

            for (var i = 1; i <= depth; i++)
            {
                sb.Append($"t{i}.ID AS ID{i}");
                if (i < depth)
                {
                    sb.Append(", ");
                }
            }

            sb.Append(" FROM ");

            for (var i = 1; i <= depth; i++)
            {
                sb.Append($"TEST t{i}");
                if (i < depth)
                {
                    sb.Append(" CROSS JOIN ");
                }
            }

            return sb.ToString();
        }

        private static void AssertInstantSimilar(Instant expected, Instant actual, string message)
        {
            double deltaSeconds = 10;

            var expectedSeconds = expected.ToUnixTimeSeconds();
            var actualSeconds = actual.ToUnixTimeSeconds();
            var diff = Math.Abs(expectedSeconds - actualSeconds);

            if (diff > deltaSeconds)
            {
                throw new InvalidOperationException(
                    $"Expected: {expectedSeconds}, actual: {actualSeconds}, diff: {diff / 3600} hours ({message})");
            }
        }
    }
}
