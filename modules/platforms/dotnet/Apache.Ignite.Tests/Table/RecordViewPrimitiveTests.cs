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

using System.Collections;
using System.Linq;
using System.Numerics;
using System.Threading.Tasks;
using Ignite.Table;
using NodaTime;
using NUnit.Framework;

/// <summary>
/// Tests record view with single-column mapping to a primitive type.
/// </summary>
public class RecordViewPrimitiveTests : IgniteTestsBase
{
    [Test]
    public async Task TestLongKey() => await TestKey(7L, Table.GetRecordView<long>());

    [Test]
    public async Task TestAllKeyTypes()
    {
        await TestKey((sbyte)1, "TBL_INT8");
        await TestKey((short)1, "TBL_INT16");
        await TestKey(1, "TBL_INT32");
        await TestKey(1L, "TBL_INT64");
        await TestKey(1.1f, "TBL_FLOAT");
        await TestKey(1.1d, "TBL_DOUBLE");
        await TestKey(1.234m, "TBL_DECIMAL");
        await TestKey("foo", "TBL_STRING");
        await TestKey(new LocalDateTime(2022, 10, 13, 8, 4, 42), "TBL_DATETIME");
        await TestKey(new LocalTime(3, 4, 5), "TBL_TIME");
        await TestKey(Instant.FromUnixTimeMilliseconds(123456789101112), "TBL_TIMESTAMP");
        await TestKey(new BigInteger(123456789101112), "TBL_NUMBER");
        await TestKey(new byte[] { 1, 2, 3 }, "TBL_BYTES");
        await TestKey(new BitArray(new[] { byte.MaxValue }), "TBL_BITMASK");
    }

    [Test]
    public void TestColumnTypeMismatchThrowsException()
    {
        var ex = Assert.ThrowsAsync<IgniteClientException>(async () => await TestKey(1f, Table.GetRecordView<float>()));
        Assert.AreEqual("Can't map 'System.Single' to column 'KEY' of type 'System.Int64' - types do not match.", ex!.Message);
    }

    [Test]
    public void TestUnmappedTypeThrowsException()
    {
        var ex = Assert.ThrowsAsync<IgniteClientException>(async () => await TestKey((byte)1, Table.GetRecordView<byte>()));
        Assert.AreEqual("Can't map 'System.Byte' to columns 'Int64 KEY, String VAL'. Matching fields not found.", ex!.Message);
    }

    private static async Task TestKey<T>(T val, IRecordView<T> recordView)
    {
        // Tests EmitWriter.
        await recordView.UpsertAsync(null, val);

        // Tests EmitValuePartReader.
        var (getRes, _) = await recordView.GetAsync(null, val);

        // Tests EmitReader.
        var getAllRes = await recordView.GetAllAsync(null, new[] { val });

        Assert.AreEqual(val, getRes);
        Assert.AreEqual(val, getAllRes.Single().Value);
    }

    private async Task TestKey<T>(T val, string tableName)
    {
        var table = await Client.Tables.GetTableAsync(tableName);

        Assert.IsNotNull(table, tableName);

        await TestKey(val, table!.GetRecordView<T>());
    }
}