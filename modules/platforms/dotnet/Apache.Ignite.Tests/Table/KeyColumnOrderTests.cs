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
/// Tests mixed key column order (before, after, intermixed with value columns).
/// </summary>
public class KeyColumnOrderTests : IgniteTestsBase
{
    // TODO: Create multiple tables with different key column orders.
    // Test with all views
    [OneTimeSetUp]
    public async Task CreateTables()
    {
        await Client.Sql.ExecuteAsync(null, "CREATE TABLE test1 (key INT PRIMARY KEY, val1 STRING, val2 STRING)");
        await Client.Sql.ExecuteAsync(null, "CREATE TABLE test2 (val1 STRING, key INT PRIMARY KEY, val2 STRING)");
        await Client.Sql.ExecuteAsync(null, "CREATE TABLE test3 (val1 STRING, val2 STRING, key INT PRIMARY KEY)");
        await Client.Sql.ExecuteAsync(null, "CREATE TABLE test4 (key INT PRIMARY KEY, val1 STRING, val2 STRING) WITH \"keyColumnOrder=last\"");
        await Client.Sql.ExecuteAsync(null, "CREATE TABLE test5 (val1 STRING, key INT PRIMARY KEY, val2 STRING) WITH \"keyColumnOrder=last\"");
        await Client.Sql.ExecuteAsync(null, "CREATE TABLE test6 (val1 STRING, val2 STRING, key INT PRIMARY KEY) WITH \"keyColumnOrder=last\"");
    }

    [OneTimeTearDown]
    public async Task DropTables()
    {
        await Client.Sql.ExecuteAsync(null, "DROP TABLE test1");
        await Client.Sql.ExecuteAsync(null, "DROP TABLE test2");
        await Client.Sql.ExecuteAsync(null, "DROP TABLE test3");
        await Client.Sql.ExecuteAsync(null, "DROP TABLE test4");
        await Client.Sql.ExecuteAsync(null, "DROP TABLE test5");
        await Client.Sql.ExecuteAsync(null, "DROP TABLE test6");
    }
}
