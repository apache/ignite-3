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
    using Ignite.Sql;
    using NUnit.Framework;

    /// <summary>
    /// Tests <see cref="SqlStatement"/>.
    /// </summary>
    public class SqlStatementTests
    {
        [Test]
        public void TestStatementCloneCreatesSeparatePropertyDictionary()
        {
            var statement1 = new SqlStatement(
                query: "select 1",
                properties: new Dictionary<string, object?> { ["foo"] = "bar" });

            var statement2 = statement1 with
            {
                Properties = new Dictionary<string, object?> { ["a"] = "b" }
            };

            Assert.AreEqual("bar", statement1.Properties["foo"]);
            Assert.AreEqual("select 1", statement2.Query);
            Assert.IsFalse(statement2.Properties.ContainsKey("foo"));
        }

        [Test]
        public void TestDefaultPropertyValues()
        {
            var s = new SqlStatement(string.Empty);

            Assert.AreEqual(SqlStatement.DefaultTimeout, s.Timeout);
            Assert.AreEqual(SqlStatement.DefaultSchema, s.Schema);
            Assert.AreEqual(SqlStatement.DefaultPageSize, s.PageSize);
        }

        [Test]
        public void TestImplicitCast()
        {
            SqlStatement s1 = "foo";
            SqlStatement s2 = SqlStatement.ToSqlStatement("foo");

            Assert.AreEqual(s1, s2);
        }

        [Test]
        public void TestToString()
        {
            var s = new SqlStatement(
                query: "select foo from bar",
                timeout: TimeSpan.FromSeconds(1),
                schema: "schema2",
                pageSize: 256,
                properties: new Dictionary<string, object?>
                {
                    { "a", 1 },
                    { "b", TimeSpan.FromHours(2) }
                });

            var expected = "SqlStatement { Query = select foo from bar, Timeout = 00:00:01, Schema = schema2, PageSize = 256, " +
                           "Properties = { a = 1, b = 02:00:00 } }";

            Assert.AreEqual(expected, s.ToString());
        }
    }
}
