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
    using System.Collections.Generic;
    using System.Threading.Tasks;
    using Ignite.Sql;
    using Ignite.Table;
    using NUnit.Framework;

    /// <summary>
    /// Tests for SQL API: <see cref="ISql"/>.
    /// </summary>
    public class SqlTests : IgniteTestsBase
    {
        [Test]
        public async Task TestSimpleQuery()
        {
            await using IResultSet resultSet = await Client.Sql.ExecuteAsync(null, "SELECT 1", 1);
            var rows = await ToListAsync(resultSet);

            Assert.AreEqual(-1, resultSet.AffectedRows);
            Assert.IsFalse(resultSet.WasApplied);
            Assert.IsTrue(resultSet.HasRowSet);

            Assert.AreEqual(
                "ResultSetMetadata { Columns = { ColumnMetadata { Name = 1, Type = Int32, Precision = 10, Scale = 0, " +
                "Nullable = False, Origin =  } } }",
                resultSet.Metadata?.ToString());

            Assert.AreEqual(1, rows.Count);
            Assert.AreEqual("TODO", rows[0]);
        }

        private static async Task<List<IIgniteTuple>> ToListAsync(IResultSet resultSet)
        {
            var res = new List<IIgniteTuple>();

            await foreach (var row in resultSet)
            {
                res.Add(row);
            }

            return res;
        }
    }
}
