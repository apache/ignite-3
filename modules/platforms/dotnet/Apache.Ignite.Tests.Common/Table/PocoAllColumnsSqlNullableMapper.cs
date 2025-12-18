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

namespace Apache.Ignite.Tests.Common.Table;

using System;
using Apache.Ignite.Table.Mapper;
using NodaTime;

public class PocoAllColumnsSqlNullableMapper : IMapper<PocoAllColumnsSqlNullable>
{
    public void Write(PocoAllColumnsSqlNullable obj, ref RowWriter rowWriter, IMapperSchema schema)
    {
        foreach (var col in schema.Columns)
        {
            switch (col.Name)
            {
                case "KEY":
                    rowWriter.WriteLong(obj.Key);
                    break;

                case "STR":
                    rowWriter.WriteString(obj.Str);
                    break;

                case "INT8":
                    rowWriter.WriteByte(obj.Int8);
                    break;

                case "INT16":
                    rowWriter.WriteShort(obj.Int16);
                    break;

                case "INT32":
                    rowWriter.WriteInt(obj.Int32);
                    break;

                case "INT64":
                    rowWriter.WriteLong(obj.Int64);
                    break;

                case "FLOAT":
                    rowWriter.WriteFloat(obj.Float);
                    break;

                case "DOUBLE":
                    rowWriter.WriteDouble(obj.Double);
                    break;

                case "DATE":
                    rowWriter.WriteDate(obj.Date);
                    break;

                case "TIME":
                    rowWriter.WriteTime(obj.Time);
                    break;

                case "DATETIME":
                    rowWriter.WriteDateTime(obj.DateTime);
                    break;

                case "TIMESTAMP":
                    rowWriter.WriteTimestamp(obj.Timestamp);
                    break;

                case "BLOB":
                    rowWriter.WriteBytes(obj.Blob);
                    break;

                case "DECIMAL":
                    rowWriter.WriteDecimal(obj.Decimal);
                    break;

                case "UUID":
                    rowWriter.WriteGuid(obj.Uuid);
                    break;

                case "BOOLEAN":
                    rowWriter.WriteBool(obj.Boolean);
                    break;

                default:
                    throw new InvalidOperationException("Unexpected column: " + col.Name);
            }
        }
    }

    public PocoAllColumnsSqlNullable Read(ref RowReader rowReader, IMapperSchema schema)
    {
        long key = default;
        string? str = null;
        sbyte? int8 = null;
        short? int16 = null;
        int? int32 = null;
        long? int64 = null;
        float? @float = null;
        double? @double = null;
        LocalDate? date = null;
        LocalTime? time = null;
        LocalDateTime? dateTime = null;
        Instant? timestamp = null;
        byte[]? blob = null;
        decimal? @decimal = null;
        Guid? uuid = null;
        bool? boolean = null;

        foreach (var col in schema.Columns)
        {
            switch (col.Name)
            {
                case "KEY":
                    key = rowReader.ReadLong()!.Value;
                    break;

                case "STR":
                    str = rowReader.ReadString();
                    break;

                case "INT8":
                    int8 = rowReader.ReadByte();
                    break;

                case "INT16":
                    int16 = rowReader.ReadShort();
                    break;

                case "INT32":
                    int32 = rowReader.ReadInt();
                    break;

                case "INT64":
                    int64 = rowReader.ReadLong();
                    break;

                case "FLOAT":
                    @float = rowReader.ReadFloat();
                    break;

                case "DOUBLE":
                    @double = rowReader.ReadDouble();
                    break;

                case "DATE":
                    date = rowReader.ReadDate();
                    break;

                case "TIME":
                    time = rowReader.ReadTime();
                    break;

                case "DATETIME":
                    dateTime = rowReader.ReadDateTime();
                    break;

                case "TIMESTAMP":
                    timestamp = rowReader.ReadTimestamp();
                    break;

                case "BLOB":
                    blob = rowReader.ReadBytes();
                    break;

                case "DECIMAL":
                    @decimal = rowReader.ReadDecimal();
                    break;

                case "UUID":
                    uuid = rowReader.ReadGuid();
                    break;

                case "BOOLEAN":
                    boolean = rowReader.ReadBool();
                    break;

                default:
                    throw new InvalidOperationException("Unexpected column: " + col.Name);
            }
        }

        return new PocoAllColumnsSqlNullable(key, str, int8, int16, int32, int64, @float, @double, date, time, dateTime, timestamp, blob, @decimal, uuid, boolean);
    }
}
