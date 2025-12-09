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
using Ignite.Table.Mapper;

public class PocoAllColumnsBigDecimalMapper : IMapper<PocoAllColumnsBigDecimal>
{
    public void Write(PocoAllColumnsBigDecimal obj, ref RowWriter rowWriter, IMapperSchema schema)
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

                case "UUID":
                    rowWriter.WriteGuid(obj.Uuid);
                    break;

                case "DECIMAL":
                    rowWriter.WriteBigDecimal(obj.Decimal);
                    break;

                default:
                    throw new InvalidOperationException("Unexpected column: " + col.Name);
            }
        }
    }

    public PocoAllColumnsBigDecimal Read(ref RowReader rowReader, IMapperSchema schema)
    {
        long key = default;
        string? str = null;
        sbyte int8 = default;
        short int16 = default;
        int int32 = default;
        long int64 = default;
        float @float = default;
        double @double = default;
        Guid uuid = default;
        BigDecimal @decimal = default;

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
                    int8 = rowReader.ReadByte()!.Value;
                    break;

                case "INT16":
                    int16 = rowReader.ReadShort()!.Value;
                    break;

                case "INT32":
                    int32 = rowReader.ReadInt()!.Value;
                    break;

                case "INT64":
                    int64 = rowReader.ReadLong()!.Value;
                    break;

                case "FLOAT":
                    @float = rowReader.ReadFloat()!.Value;
                    break;

                case "DOUBLE":
                    @double = rowReader.ReadDouble()!.Value;
                    break;

                case "UUID":
                    uuid = rowReader.ReadGuid()!.Value;
                    break;

                case "DECIMAL":
                    @decimal = rowReader.ReadBigDecimal()!.Value;
                    break;

                default:
                    throw new InvalidOperationException("Unexpected column: " + col.Name);
            }
        }

        return new PocoAllColumnsBigDecimal(key, str, int8, int16, int32, int64, @float, @double, uuid, @decimal);
    }
}
