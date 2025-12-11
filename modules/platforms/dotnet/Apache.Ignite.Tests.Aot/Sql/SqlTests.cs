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

namespace Apache.Ignite.Tests.Aot.Sql;

using Ignite.Sql;
using Ignite.Table;
using JetBrains.Annotations;

public class SqlTests(IIgniteClient client)
{
    [UsedImplicitly]
    public async Task TestSimpleQuery()
    {
        await using IResultSet<IIgniteTuple> resultSet = await client.Sql.ExecuteAsync(null, "select 1 as num, 'hello' as str");
        var rows = await resultSet.ToListAsync();

        Assert.AreEqual(-1L, resultSet.AffectedRows);
        Assert.AreEqual(false, resultSet.WasApplied);
        Assert.AreEqual(true, resultSet.HasRowSet);

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
}
