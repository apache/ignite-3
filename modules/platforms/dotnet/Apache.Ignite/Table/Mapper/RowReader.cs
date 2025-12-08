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

namespace Apache.Ignite.Table.Mapper;

using System;
using Internal.Proto.BinaryTuple;
using Internal.Table;
using NodaTime;
using Sql;

/// <summary>
/// Row reader for mappers. Reads columns in the order defined by the schema.
/// </summary>
public ref struct RowReader
{
    private readonly BinaryTupleReader _reader;

    private readonly Column[] _columns;

    private int _position = -1;

    /// <summary>
    /// Initializes a new instance of the <see cref="RowReader"/> struct.
    /// </summary>
    /// <param name="reader">Reader.</param>
    /// <param name="columns">Columns.</param>
    internal RowReader(ref BinaryTupleReader reader, Column[] columns)
    {
        _reader = reader;
        _columns = columns;
    }

    private readonly Column Column
    {
        get
        {
            if (_position >= _columns.Length)
            {
                throw new IgniteClientException(
                    ErrorGroups.Client.Configuration,
                    "Attempted to read more columns than defined in the schema.");
            }

            return _columns[_position];
        }
    }

    /// <summary>
    /// Reads the next column as a byte.
    /// </summary>
    /// <returns>Column value.</returns>
    [CLSCompliant(false)]
    public sbyte? ReadByte()
    {
        AdvanceAndCheckColumnType(ColumnType.Int8);

        return _reader.GetByteNullable(_position);
    }

    /// <summary>
    /// Reads the next column as a boolean.
    /// </summary>
    /// <returns>Column value.</returns>
    public bool? ReadBool()
    {
        AdvanceAndCheckColumnType(ColumnType.Boolean);

        return _reader.GetBoolNullable(_position);
    }

    /// <summary>
    /// Reads the next column as a short.
    /// </summary>
    /// <returns>Column value.</returns>
    public short? ReadShort()
    {
        AdvanceAndCheckColumnType(ColumnType.Int16);

        return _reader.GetShortNullable(_position);
    }

    /// <summary>
    /// Reads the next column as an int.
    /// </summary>
    /// <returns>Column value.</returns>
    public int? ReadInt()
    {
        AdvanceAndCheckColumnType(ColumnType.Int32);

        return _reader.GetIntNullable(_position);
    }

    /// <summary>
    /// Reads the next column as a long.
    /// </summary>
    /// <returns>Column value.</returns>
    public long? ReadLong()
    {
        AdvanceAndCheckColumnType(ColumnType.Int64);

        return _reader.GetLongNullable(_position);
    }

    /// <summary>
    /// Reads the next column as a float.
    /// </summary>
    /// <returns>Column value.</returns>
    public float? ReadFloat()
    {
        AdvanceAndCheckColumnType(ColumnType.Float);

        return _reader.GetFloatNullable(_position);
    }

    /// <summary>
    /// Reads the next column as a double.
    /// </summary>
    /// <returns>Column value.</returns>
    public double? ReadDouble()
    {
        AdvanceAndCheckColumnType(ColumnType.Double);

        return _reader.GetDoubleNullable(_position);
    }

    /// <summary>
    /// Reads the next column as a string.
    /// </summary>
    /// <returns>Column value.</returns>
    public string? ReadString()
    {
        AdvanceAndCheckColumnType(ColumnType.String);

        return _reader.GetStringNullable(_position);
    }

    /// <summary>
    /// Reads the next column as a byte array.
    /// </summary>
    /// <returns>Column value.</returns>
    public byte[]? ReadBytes()
    {
        AdvanceAndCheckColumnType(ColumnType.ByteArray);

        return _reader.GetBytesNullable(_position);
    }

    /// <summary>
    /// Reads the next column as a GUID.
    /// </summary>
    /// <returns>Column value.</returns>
    public Guid? ReadGuid()
    {
        AdvanceAndCheckColumnType(ColumnType.Uuid);

        return _reader.GetGuidNullable(_position);
    }

    /// <summary>
    /// Reads the next column as a decimal.
    /// </summary>
    /// <returns>Column value.</returns>
    public decimal? ReadDecimal()
    {
        AdvanceAndCheckColumnType(ColumnType.Decimal);

        return _reader.GetDecimalNullable(_position, Column.Scale);
    }

    /// <summary>
    /// Reads the next column as a big decimal.
    /// </summary>
    /// <returns>Column value.</returns>
    public BigDecimal? ReadBigDecimal()
    {
        AdvanceAndCheckColumnType(ColumnType.Decimal);

        return _reader.GetBigDecimalNullable(_position, Column.Scale);
    }

    /// <summary>
    /// Reads the next column as a date.
    /// </summary>
    /// <returns>Column value.</returns>
    public LocalDate? ReadDate()
    {
        AdvanceAndCheckColumnType(ColumnType.Date);

        return _reader.GetDateNullable(_position);
    }

    /// <summary>
    /// Reads the next column as a time.
    /// </summary>
    /// <returns>Column value.</returns>
    public LocalTime? ReadTime()
    {
        AdvanceAndCheckColumnType(ColumnType.Time);

        return _reader.GetTimeNullable(_position);
    }

    /// <summary>
    /// Reads the next column as a date and time.
    /// </summary>
    /// <returns>Column value.</returns>
    public LocalDateTime? ReadDateTime()
    {
        AdvanceAndCheckColumnType(ColumnType.Datetime);

        return _reader.GetDateTimeNullable(_position);
    }

    /// <summary>
    /// Reads the next column as a timestamp (instant).
    /// </summary>
    /// <returns>Column value.</returns>
    public Instant? ReadTimestamp()
    {
        AdvanceAndCheckColumnType(ColumnType.Timestamp);

        return _reader.GetTimestampNullable(_position);
    }

    /// <summary>
    /// Reads the next column as a duration.
    /// </summary>
    /// <returns>Column value.</returns>
    public Duration? ReadDuration()
    {
        AdvanceAndCheckColumnType(ColumnType.Duration);

        return _reader.GetDurationNullable(_position);
    }

    /// <summary>
    /// Reads the next column as a period.
    /// </summary>
    /// <returns>Column value.</returns>
    public Period? ReadPeriod()
    {
        AdvanceAndCheckColumnType(ColumnType.Period);

        return _reader.GetPeriodNullable(_position);
    }

    /// <summary>
    /// Skips the current column.
    /// </summary>
    public void Skip() => ++_position;

    private void AdvanceAndCheckColumnType(ColumnType provided)
    {
        ++_position;
        var col = Column;

        if (col.Type == provided)
        {
            return;
        }

        throw new IgniteClientException(
            ErrorGroups.Client.Configuration,
            $"Can't read a value of type '{provided}' from column '{col.Name}' of type '{col.Type}'.");
    }
}
