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
using Internal.Table.Serialization;
using NodaTime;
using Sql;

/// <summary>
/// Row writer for mappers. Writes columns in the order defined by the schema.
/// </summary>
public ref struct RowWriter
{
    private readonly Span<byte> _noValueSet;

    private readonly Column[] _columns;

    private BinaryTupleBuilder _builder;

    /// <summary>
    /// Initializes a new instance of the <see cref="RowWriter"/> struct.
    /// </summary>
    /// <param name="builder">Builder.</param>
    /// <param name="noValueSet">No-value set.</param>
    /// <param name="columns">Columns.</param>
    internal RowWriter(ref BinaryTupleBuilder builder, Span<byte> noValueSet, Column[] columns)
    {
        _builder = builder;
        _noValueSet = noValueSet;
        _columns = columns;
    }

    /// <summary>
    /// Gets the builder.
    /// </summary>
    internal readonly BinaryTupleBuilder Builder => _builder;

    private Column Column
    {
        get
        {
            if (_builder.ElementIndex >= _columns.Length)
            {
                throw new IgniteClientException(
                    ErrorGroups.Client.Configuration,
                    "Attempted to write more columns than defined in the schema.");
            }

            return _columns[_builder.ElementIndex];
        }
    }

    /// <summary>
    /// Writes a byte value.
    /// </summary>
    /// <param name="value">Value.</param>
    [CLSCompliant(false)]
    public void WriteByte(sbyte? value)
    {
        CheckColumnType(ColumnType.Int8);

        _builder.AppendByteNullable(value);
    }

    /// <summary>
    /// Writes a boolean value.
    /// </summary>
    /// <param name="value">Value.</param>
    public void WriteBool(bool? value)
    {
        CheckColumnType(ColumnType.Boolean);

        _builder.AppendBoolNullable(value);
    }

    /// <summary>
    /// Writes a short value.
    /// </summary>
    /// <param name="value">Value.</param>
    public void WriteShort(short? value)
    {
        CheckColumnType(ColumnType.Int16);

        _builder.AppendShortNullable(value);
    }

    /// <summary>
    /// Writes an integer value.
    /// </summary>
    /// <param name="value">Value.</param>
    public void WriteInt(int? value)
    {
        CheckColumnType(ColumnType.Int32);

        _builder.AppendIntNullable(value);
    }

    /// <summary>
    /// Writes a long value.
    /// </summary>
    /// <param name="value">Value.</param>
    public void WriteLong(long? value)
    {
        CheckColumnType(ColumnType.Int64);

        _builder.AppendLongNullable(value);
    }

    /// <summary>
    /// Writes a float value.
    /// </summary>
    /// <param name="value">Value.</param>
    public void WriteFloat(float? value)
    {
        CheckColumnType(ColumnType.Float);

        _builder.AppendFloatNullable(value);
    }

    /// <summary>
    /// Writes a double value.
    /// </summary>
    /// <param name="value">Value.</param>
    public void WriteDouble(double? value)
    {
        CheckColumnType(ColumnType.Double);

        _builder.AppendDoubleNullable(value);
    }

    /// <summary>
    /// Writes a string value.
    /// </summary>
    /// <param name="value">Value.</param>
    public void WriteString(string? value)
    {
        CheckColumnType(ColumnType.String);

        _builder.AppendStringNullable(value);
    }

    /// <summary>
    /// Writes a byte array value.
    /// </summary>
    /// <param name="value">Value.</param>
    public void WriteBytes(byte[]? value)
    {
        CheckColumnType(ColumnType.ByteArray);

        _builder.AppendBytesNullable(value);
    }

    /// <summary>
    /// Writes a GUID value.
    /// </summary>
    /// <param name="value">Value.</param>
    public void WriteGuid(Guid? value)
    {
        CheckColumnType(ColumnType.Uuid);

        _builder.AppendGuidNullable(value);
    }

    /// <summary>
    /// Writes a decimal value.
    /// </summary>
    /// <param name="value">Value.</param>
    public void WriteDecimal(decimal? value)
    {
        CheckColumnType(ColumnType.Decimal);

        _builder.AppendDecimalNullable(value, Column.Scale);
    }

    /// <summary>
    /// Writes a big decimal value.
    /// </summary>
    /// <param name="value">Value.</param>
    public void WriteBigDecimal(BigDecimal? value)
    {
        CheckColumnType(ColumnType.Decimal);

        _builder.AppendBigDecimalNullable(value, Column.Scale);
    }

    /// <summary>
    /// Writes a date value.
    /// </summary>
    /// <param name="value">Value.</param>
    public void WriteDate(LocalDate? value)
    {
        CheckColumnType(ColumnType.Date);

        _builder.AppendDateNullable(value);
    }

    /// <summary>
    /// Writes a time value.
    /// </summary>
    /// <param name="value">Value.</param>
    public void WriteTime(LocalTime? value)
    {
        CheckColumnType(ColumnType.Time);

        _builder.AppendTimeNullable(value, Column.Precision);
    }

    /// <summary>
    /// Writes a date and time value.
    /// </summary>
    /// <param name="value">Value.</param>
    public void WriteDateTime(LocalDateTime? value)
    {
        CheckColumnType(ColumnType.Datetime);

        _builder.AppendDateTimeNullable(value, Column.Precision);
    }

    /// <summary>
    /// Writes a timestamp (instant) value.
    /// </summary>
    /// <param name="value">Value.</param>
    public void WriteTimestamp(Instant? value)
    {
        CheckColumnType(ColumnType.Timestamp);

        _builder.AppendTimestampNullable(value, Column.Precision);
    }

    /// <summary>
    /// Writes a duration value.
    /// </summary>
    /// <param name="value">Value.</param>
    public void WriteDuration(Duration? value)
    {
        CheckColumnType(ColumnType.Duration);

        _builder.AppendDurationNullable(value);
    }

    /// <summary>
    /// Writes a period value.
    /// </summary>
    /// <param name="value">Value.</param>
    public void WritePeriod(Period? value)
    {
        CheckColumnType(ColumnType.Period);

        _builder.AppendPeriodNullable(value);
    }

    /// <summary>
    /// Skips writing the next column (marks as not set, so that the default column value can be applied by the server).
    /// </summary>
    public void Skip() => _builder.AppendNoValue(_noValueSet);

    private void CheckColumnType(ColumnType provided)
    {
        var col = Column;

        if (col.Type == provided)
        {
            return;
        }

        throw new IgniteClientException(
            ErrorGroups.Client.Configuration,
            $"Can't write a value of type '{provided}' to column '{col.Name}' of type '{col.Type}'.");
    }
}
