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

using System;
using System.Linq;
using System.Threading.Tasks;
using Ignite.Sql;
using Ignite.Table;
using Ignite.Table.Mapper;
using NodaTime;
using NUnit.Framework;
using static Common.Table.TestTables;

/// <summary>
/// Tests record view with single-column mapping to a primitive type.
/// </summary>
[TestFixture("reflective")]
[TestFixture("mapper")]
public class RecordViewPrimitiveTests(string mode) : IgniteTestsBase(useMapper: mode == "mapper")
{
    [Test]
    public async Task TestLongKey() => await TestKey(7L, Table.GetRecordView<long>());

    [Test]
    public async Task TestAllKeyTypes()
    {
        await TestKey((sbyte)1, TableInt8Name);
        await TestKey(true, TableBoolName);
        await TestKey((short)1, TableInt16Name);
        await TestKey(1, TableInt32Name);
        await TestKey(1L, TableInt64Name);
        await TestKey(1.1f, TableFloatName);
        await TestKey(1.1d, TableDoubleName);
        await TestKey(1.234m, TableDecimalName);
        await TestKey(new BigDecimal(1.234m), TableDecimalName);
        await TestKey("foo", TableStringName);
        await TestKey(new LocalDateTime(2022, 10, 13, 8, 4, 42), TableDateTimeName);
        await TestKey(new LocalTime(3, 4, 5), TableTimeName);
        await TestKey(Instant.FromUnixTimeMilliseconds(123456789101112), TableTimestampName);
        await TestKey(new byte[] { 1, 2, 3 }, TableBytesName);
        await TestKey(Guid.NewGuid(), TableUuidName);
    }

    [Test]
    public void TestColumnTypeMismatchThrowsException()
    {
        var ex = Assert.ThrowsAsync<IgniteClientException>(async () => await TestKey(1f, Table.GetRecordView<float>()));
        Assert.AreEqual("Can't write a value of type 'Float' to column 'KEY' of type 'Int64'.", ex!.Message);
    }

    [Test]
    public void TestUnmappedTypeThrowsException()
    {
        var ex = Assert.ThrowsAsync<ArgumentException>(async () => await TestKey((byte)1, Table.GetRecordView<byte>()));
        Assert.AreEqual("Can't map 'System.Byte' to columns 'Int64 KEY, String VAL'. Matching fields not found.", ex!.Message);
    }

    private static async Task TestKey<T>(T val, IRecordView<T> recordView)
        where T : notnull
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
        where T : notnull
    {
        var table = await Client.Tables.GetTableAsync(tableName);

        Assert.IsNotNull(table, "Table must exist: " + tableName);

        var recordView = UseMapper
            ? table!.GetRecordView(new PrimitiveMapper<T>())
            : table!.GetRecordView<T>();

        await TestKey(val, recordView);
    }

    private class PrimitiveMapper<T> : IMapper<T>
        where T : notnull
    {
        public void Write(T obj, ref RowWriter rowWriter, IMapperSchema schema)
        {
            var col = schema.Columns[0];

            switch (col.Type)
            {
                case ColumnType.Boolean:
                    rowWriter.WriteBool((bool)(object)obj);
                    break;
                case ColumnType.Int8:
                    rowWriter.WriteByte((sbyte)(object)obj);
                    break;
                case ColumnType.Int16:
                    rowWriter.WriteShort((short)(object)obj);
                    break;
                case ColumnType.Int32:
                    rowWriter.WriteInt((int)(object)obj);
                    break;
                case ColumnType.Int64:
                    rowWriter.WriteLong((long)(object)obj);
                    break;
                case ColumnType.Float:
                    rowWriter.WriteFloat((float)(object)obj);
                    break;
                case ColumnType.Double:
                    rowWriter.WriteDouble((double)(object)obj);
                    break;
                case ColumnType.Decimal:
                    if (obj is BigDecimal bd)
                    {
                        rowWriter.WriteBigDecimal(bd);
                    }
                    else
                    {
                        rowWriter.WriteDecimal((decimal)(object)obj);
                    }

                    break;
                case ColumnType.String:
                    rowWriter.WriteString((string)(object)obj);
                    break;
                case ColumnType.Date:
                    rowWriter.WriteDate((LocalDate)(object)obj);
                    break;
                case ColumnType.Time:
                    rowWriter.WriteTime((LocalTime)(object)obj);
                    break;
                case ColumnType.Datetime:
                    rowWriter.WriteDateTime((LocalDateTime)(object)obj);
                    break;
                case ColumnType.Timestamp:
                    rowWriter.WriteTimestamp((Instant)(object)obj);
                    break;
                case ColumnType.Uuid:
                    rowWriter.WriteGuid((Guid)(object)obj);
                    break;
                case ColumnType.ByteArray:
                    rowWriter.WriteBytes((byte[])(object)obj);
                    break;
                default:
                    Assert.Fail("Unsupported column type: " + col.Type);
                    break;
            }

            for (int i = 1; i < schema.Columns.Count; i++)
            {
                rowWriter.Skip();
            }
        }

        public T Read(ref RowReader rowReader, IMapperSchema schema)
        {
            var col = schema.Columns[0];

            return col.Type switch
            {
                ColumnType.Boolean => (T)(object)rowReader.ReadBool()!,
                ColumnType.Int8 => (T)(object)rowReader.ReadByte()!,
                ColumnType.Int16 => (T)(object)rowReader.ReadShort()!,
                ColumnType.Int32 => (T)(object)rowReader.ReadInt()!,
                ColumnType.Int64 => (T)(object)rowReader.ReadLong()!,
                ColumnType.Float => (T)(object)rowReader.ReadFloat()!,
                ColumnType.Double => (T)(object)rowReader.ReadDouble()!,
                ColumnType.Decimal => typeof(T) == typeof(BigDecimal)
                    ? (T)(object)rowReader.ReadBigDecimal()!
                    : (T)(object)rowReader.ReadDecimal()!,
                ColumnType.String => (T)(object)rowReader.ReadString()!,
                ColumnType.Date => (T)(object)rowReader.ReadDate()!,
                ColumnType.Time => (T)(object)rowReader.ReadTime()!,
                ColumnType.Datetime => (T)(object)rowReader.ReadDateTime()!,
                ColumnType.Timestamp => (T)(object)rowReader.ReadTimestamp()!,
                ColumnType.Uuid => (T)(object)rowReader.ReadGuid()!,
                ColumnType.ByteArray => (T)(object)rowReader.ReadBytes()!,

                // ReSharper disable PatternIsRedundant
                ColumnType.Null or ColumnType.Period or ColumnType.Duration or _ => default!
            };
        }
    }
}
