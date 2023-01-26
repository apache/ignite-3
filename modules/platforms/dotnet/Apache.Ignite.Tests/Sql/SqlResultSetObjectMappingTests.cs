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

namespace Apache.Ignite.Tests.Sql;

using System;
using System.Threading.Tasks;
using Ignite.Sql;
using NodaTime;
using NUnit.Framework;
using Table;

/// <summary>
/// Tests result set object mapping with <see cref="ISql.ExecuteAsync{T}"/>.
/// </summary>
public class SqlResultSetObjectMappingTests : IgniteTestsBase
{
    private const int Count = 5;

    [OneTimeSetUp]
    public async Task InsertData()
    {
        for (int i = 0; i < Count; i++)
        {
            var poco = new PocoAllColumnsSqlNullable(
                i,
                "v -" + i,
                (sbyte)(i + 1),
                (short)(i + 2),
                i + 3,
                i + 4,
                i + 5.5f,
                i + 6.5,
                new LocalDate(2022, 12, i + 1),
                new LocalTime(11, 38, i + 1),
                new LocalDateTime(2022, 12, 19, 11, i + 1),
                Instant.FromUnixTimeSeconds(i + 1),
                new byte[] { 1, 2 },
                i + 7.7m);

            await PocoAllColumnsSqlNullableView.UpsertAsync(null, poco);
        }

        await PocoAllColumnsSqlNullableView.UpsertAsync(null, new PocoAllColumnsSqlNullable(100));
    }

    [Test]
    public async Task TestPrimitiveMapping()
    {
        var resultSet = await Client.Sql.ExecuteAsync<int>(null, "select INT32 from TBL_ALL_COLUMNS_SQL WHERE INT32 is not null order by 1");
        var rows = await resultSet.ToListAsync();

        Assert.AreEqual(Count, rows.Count);
    }

    [Test]
    public async Task TestSelectNullIntoNonNullablePrimitiveTypeThrows()
    {
        // TODO: Same test with a field.
        var resultSet = await Client.Sql.ExecuteAsync<int>(null, "select INT32 from TBL_ALL_COLUMNS_SQL");

        var ex = Assert.ThrowsAsync<InvalidOperationException>(async () => await resultSet.ToListAsync());

        Assert.AreEqual(
            "Can not read NULL from column 'INT32' of type 'System.Nullable`1[System.Int32]' into type 'System.Int32'.",
            ex!.Message);
    }
}
