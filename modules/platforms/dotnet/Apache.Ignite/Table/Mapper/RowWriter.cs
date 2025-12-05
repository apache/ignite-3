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
using Internal.Table.Serialization;
using NodaTime;

/// <summary>
/// Row writer for mappers. Writes columns in the order defined by the schema.
/// </summary>
public ref struct RowWriter
{
    private readonly Span<byte> _noValueSet;

    private BinaryTupleBuilder _builder;

    /// <summary>
    /// Initializes a new instance of the <see cref="RowWriter"/> struct.
    /// </summary>
    /// <param name="builder">Builder.</param>
    /// <param name="noValueSet">No-value set.</param>
    internal RowWriter(ref BinaryTupleBuilder builder, Span<byte> noValueSet)
    {
        _builder = builder;
        _noValueSet = noValueSet;
    }

    /// <summary>
    /// Gets the builder.
    /// </summary>
    internal readonly BinaryTupleBuilder Builder => _builder;

    /// <summary>
    /// Writes a byte value.
    /// </summary>
    /// <param name="value">Value.</param>
    [CLSCompliant(false)]
    public void WriteByte(sbyte? value) => _builder.AppendByteNullable(value);

    /// <summary>
    /// Writes a boolean value.
    /// </summary>
    /// <param name="value">Value.</param>
    public void WriteBool(bool? value) => _builder.AppendBoolNullable(value);

    /// <summary>
    /// Writes a short value.
    /// </summary>
    /// <param name="value">Value.</param>
    public void WriteShort(short? value) => _builder.AppendShortNullable(value);

    /// <summary>
    /// Writes an integer value.
    /// </summary>
    /// <param name="value">Value.</param>
    public void WriteInt(int? value) => _builder.AppendIntNullable(value);

    /// <summary>
    /// Writes a long value.
    /// </summary>
    /// <param name="value">Value.</param>
    public void WriteLong(long? value) => _builder.AppendLongNullable(value);

    /// <summary>
    /// Writes a float value.
    /// </summary>
    /// <param name="value">Value.</param>
    public void WriteFloat(float? value) => _builder.AppendFloatNullable(value);

    /// <summary>
    /// Writes a double value.
    /// </summary>
    /// <param name="value">Value.</param>
    public void WriteDouble(double? value) => _builder.AppendDoubleNullable(value);

    /// <summary>
    /// Writes a string value.
    /// </summary>
    /// <param name="value">Value.</param>
    public void WriteString(string? value) => _builder.AppendStringNullable(value);

    /// <summary>
    /// Writes a byte array value.
    /// </summary>
    /// <param name="value">Value.</param>
    public void WriteBytes(byte[]? value) => _builder.AppendBytesNullable(value);

    /// <summary>
    /// Writes a GUID value.
    /// </summary>
    /// <param name="value">Value.</param>
    public void WriteGuid(Guid? value) => _builder.AppendGuidNullable(value);

    /// <summary>
    /// Writes a decimal value.
    /// </summary>
    /// <param name="value">Value.</param>
    /// <param name="scale">Decimal scale from schema.</param>
    public void WriteDecimal(decimal? value, int scale) => _builder.AppendDecimalNullable(value, scale);  // TODO: Remove scale.

    /// <summary>
    /// Writes a big decimal value.
    /// </summary>
    /// <param name="value">Value.</param>
    /// <param name="scale">Decimal scale from schema.</param>
    public void WriteBigDecimal(BigDecimal? value, int scale) => _builder.AppendBigDecimalNullable(value, scale);

    /// <summary>
    /// Writes a date value.
    /// </summary>
    /// <param name="value">Value.</param>
    public void WriteDate(LocalDate? value) => _builder.AppendDateNullable(value);

    /// <summary>
    /// Writes a time value.
    /// </summary>
    /// <param name="value">Value.</param>
    /// <param name="precision">Precision.</param>
    public void WriteTime(LocalTime? value, int precision) => _builder.AppendTimeNullable(value, precision);

    /// <summary>
    /// Writes a date and time value.
    /// </summary>
    /// <param name="value">Value.</param>
    /// <param name="precision">Precision.</param>
    public void WriteDateTime(LocalDateTime? value, int precision) => _builder.AppendDateTimeNullable(value, precision);

    /// <summary>
    /// Writes a timestamp (instant) value.
    /// </summary>
    /// <param name="value">Value.</param>
    /// <param name="precision">Precision.</param>
    public void WriteTimestamp(Instant? value, int precision) => _builder.AppendTimestampNullable(value, precision);

    /// <summary>
    /// Writes a duration value.
    /// </summary>
    /// <param name="value">Value.</param>
    public void WriteDuration(Duration? value) => _builder.AppendDurationNullable(value);

    /// <summary>
    /// Writes a period value.
    /// </summary>
    /// <param name="value">Value.</param>
    public void WritePeriod(Period? value) => _builder.AppendPeriodNullable(value);

    /// <summary>
    /// Skips writing the next column (marks as not set, so that the default column value can be applied by the server).
    /// </summary>
    public void Skip() => _builder.AppendNoValue(_noValueSet);
}
