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

namespace Apache.Ignite.Internal.Proto.BinaryTuple
{
    using System;
    using System.Buffers.Binary;
    using System.Collections;
    using System.Diagnostics;
    using System.Numerics;
    using Ignite.Sql;
    using NodaTime;

    /// <summary>
    /// Binary tuple reader.
    /// </summary>
    internal readonly ref struct BinaryTupleReader
    {
        /** Buffer. */
        private readonly ReadOnlySpan<byte> _buffer;

        /** Number of elements in the tuple. */
        private readonly int _numElements;

        /** Size of an offset table entry. */
        private readonly int _entrySize;

        /** Position of the varlen offset table. */
        private readonly int _entryBase;

        /** Starting position of variable-length values. */
        private readonly int _valueBase;

        /// <summary>
        /// Initializes a new instance of the <see cref="BinaryTupleReader"/> struct.
        /// </summary>
        /// <param name="buffer">Buffer.</param>
        /// <param name="numElements">Number of elements in the tuple.</param>
        public BinaryTupleReader(ReadOnlySpan<byte> buffer, int numElements)
        {
            _buffer = buffer;
            _numElements = numElements;

            var flags = buffer[0];

            int @base = BinaryTupleCommon.HeaderSize;

            if ((flags & BinaryTupleCommon.NullmapFlag) != 0)
            {
                @base += BinaryTupleCommon.NullMapSize(numElements);
            }

            _entryBase = @base;
            _entrySize = 1 << (flags & BinaryTupleCommon.VarsizeMask);
            _valueBase = @base + _entrySize * numElements;
        }

        /// <summary>
        /// Gets a value indicating whether this reader has a null map.
        /// </summary>
        public bool HasNullMap => _entryBase > BinaryTupleCommon.HeaderSize;

        /// <summary>
        /// Gets a value indicating whether the element at specified index is null.
        /// </summary>
        /// <param name="index">Element index.</param>
        /// <returns>True when the element is null; false otherwise.</returns>
        public bool IsNull(int index)
        {
            if (!HasNullMap)
            {
                return false;
            }

            int nullIndex = BinaryTupleCommon.NullOffset(index);
            byte nullMask = BinaryTupleCommon.NullMask(index);

            return (_buffer[nullIndex] & nullMask) != 0;
        }

        /// <summary>
        /// Gets a byte value.
        /// </summary>
        /// <param name="index">Index.</param>
        /// <returns>Value.</returns>
        public sbyte GetByte(int index) => Seek(index) switch
        {
            { IsEmpty: true } => default,
            { Length: 1 } s => unchecked((sbyte)s[0]),
            var s => throw GetInvalidLengthException(index, 1, s.Length)
        };

        /// <summary>
        /// Gets a byte value.
        /// </summary>
        /// <param name="index">Index.</param>
        /// <returns>Value.</returns>
        public sbyte? GetByteNullable(int index) => IsNull(index) ? null : GetByte(index);

        /// <summary>
        /// Gets a byte value as bool.
        /// </summary>
        /// <param name="index">Index.</param>
        /// <returns>Value.</returns>
        public bool GetByteAsBool(int index) => GetByte(index) != 0;

        /// <summary>
        /// Gets a byte value as bool.
        /// </summary>
        /// <param name="index">Index.</param>
        /// <returns>Value.</returns>
        public bool? GetByteAsBoolNullable(int index) => IsNull(index) ? null : GetByte(index) != 0;

        /// <summary>
        /// Gets a short value.
        /// </summary>
        /// <param name="index">Index.</param>
        /// <returns>Value.</returns>
        public short GetShort(int index) => Seek(index) switch
        {
            { IsEmpty: true } => default,
            { Length: 1 } s => unchecked((sbyte)s[0]),
            { Length: 2 } s => BinaryPrimitives.ReadInt16LittleEndian(s),
            var s => throw GetInvalidLengthException(index, 2, s.Length)
        };

        /// <summary>
        /// Gets a short value.
        /// </summary>
        /// <param name="index">Index.</param>
        /// <returns>Value.</returns>
        public short? GetShortNullable(int index) => IsNull(index) ? null : GetShort(index);

        /// <summary>
        /// Gets an int value.
        /// </summary>
        /// <param name="index">Index.</param>
        /// <returns>Value.</returns>
        public int GetInt(int index) => Seek(index) switch
        {
            { IsEmpty: true } => default,
            { Length: 1 } s => unchecked((sbyte)s[0]),
            { Length: 2 } s => BinaryPrimitives.ReadInt16LittleEndian(s),
            { Length: 4 } s => BinaryPrimitives.ReadInt32LittleEndian(s),
            var s => throw GetInvalidLengthException(index, 4, s.Length)
        };

        /// <summary>
        /// Gets an int value.
        /// </summary>
        /// <param name="index">Index.</param>
        /// <returns>Value.</returns>
        public int? GetIntNullable(int index) => IsNull(index) ? null : GetInt(index);

        /// <summary>
        /// Gets a long value.
        /// </summary>
        /// <param name="index">Index.</param>
        /// <returns>Value.</returns>
        public long GetLong(int index) => Seek(index) switch
        {
            { IsEmpty: true } => default,
            { Length: 1 } s => unchecked((sbyte)s[0]),
            { Length: 2 } s => BinaryPrimitives.ReadInt16LittleEndian(s),
            { Length: 4 } s => BinaryPrimitives.ReadInt32LittleEndian(s),
            { Length: 8 } s => BinaryPrimitives.ReadInt64LittleEndian(s),
            var s => throw GetInvalidLengthException(index, 8, s.Length)
        };

        /// <summary>
        /// Gets a long value.
        /// </summary>
        /// <param name="index">Index.</param>
        /// <returns>Value.</returns>
        public long? GetLongNullable(int index) => IsNull(index) ? null : GetLong(index);

        /// <summary>
        /// Gets a Guid value.
        /// </summary>
        /// <param name="index">Index.</param>
        /// <returns>Value.</returns>
        public Guid GetGuid(int index) => Seek(index) switch
        {
            { IsEmpty: true } => default,
            { Length: 16 } s => UuidSerializer.Read(s),
            var s => throw GetInvalidLengthException(index, 16, s.Length)
        };

        /// <summary>
        /// Gets a Guid value.
        /// </summary>
        /// <param name="index">Index.</param>
        /// <returns>Value.</returns>
        public Guid? GetGuidNullable(int index) => IsNull(index) ? null : GetGuid(index);

        /// <summary>
        /// Gets a string value.
        /// </summary>
        /// <param name="index">Index.</param>
        /// <returns>Value.</returns>
        public string GetString(int index) => Seek(index) switch
        {
            { IsEmpty: true } => string.Empty,
            var s => ProtoCommon.StringEncoding.GetString(s)
        };

        /// <summary>
        /// Gets a string value.
        /// </summary>
        /// <param name="index">Index.</param>
        /// <returns>Value.</returns>
        public string? GetStringNullable(int index) => IsNull(index) ? null : GetString(index);

        /// <summary>
        /// Gets a float value.
        /// </summary>
        /// <param name="index">Index.</param>
        /// <returns>Value.</returns>
        public float GetFloat(int index) => Seek(index) switch
        {
            { IsEmpty: true } => default,
            { Length: 4 } s => BitConverter.Int32BitsToSingle(BinaryPrimitives.ReadInt32LittleEndian(s)),
            var s => throw GetInvalidLengthException(index, 4, s.Length)
        };

        /// <summary>
        /// Gets a float value.
        /// </summary>
        /// <param name="index">Index.</param>
        /// <returns>Value.</returns>
        public float? GetFloatNullable(int index) => IsNull(index) ? null : GetFloat(index);

        /// <summary>
        /// Gets a double value.
        /// </summary>
        /// <param name="index">Index.</param>
        /// <returns>Value.</returns>
        public double GetDouble(int index) => Seek(index) switch
        {
            { IsEmpty: true } => default,
            { Length: 4 } s => BitConverter.Int32BitsToSingle(BinaryPrimitives.ReadInt32LittleEndian(s)),
            { Length: 8 } s => BitConverter.Int64BitsToDouble(BinaryPrimitives.ReadInt64LittleEndian(s)),
            var s => throw GetInvalidLengthException(index, 8, s.Length)
        };

        /// <summary>
        /// Gets a double value.
        /// </summary>
        /// <param name="index">Index.</param>
        /// <returns>Value.</returns>
        public double? GetDoubleNullable(int index) => IsNull(index) ? null : GetDouble(index);

        /// <summary>
        /// Gets a bit mask value.
        /// </summary>
        /// <param name="index">Index.</param>
        /// <returns>Value.</returns>
        public BitArray GetBitmask(int index) => Seek(index) switch
        {
            { IsEmpty: true } => new BitArray(0),
            var s => new BitArray(s.ToArray())
        };

        /// <summary>
        /// Gets a bit mask value.
        /// </summary>
        /// <param name="index">Index.</param>
        /// <returns>Value.</returns>
        public BitArray? GetBitmaskNullable(int index) => IsNull(index) ? null : GetBitmask(index);

        /// <summary>
        /// Gets a decimal value.
        /// </summary>
        /// <param name="index">Index.</param>
        /// <param name="scale">Decimal scale.</param>
        /// <returns>Value.</returns>
        public decimal GetDecimal(int index, int scale) => Seek(index) switch
        {
            { IsEmpty: true } => default,
            var s => ReadDecimal(s, scale)
        };

        /// <summary>
        /// Gets a decimal value.
        /// </summary>
        /// <param name="index">Index.</param>
        /// <param name="scale">Decimal scale.</param>
        /// <returns>Value.</returns>
        public decimal? GetDecimalNullable(int index, int scale) => IsNull(index) ? null : GetDecimal(index, scale);

        /// <summary>
        /// Gets a number (big integer) value.
        /// </summary>
        /// <param name="index">Index.</param>
        /// <returns>Value.</returns>
        public BigInteger GetNumber(int index) => Seek(index) switch
        {
            { IsEmpty: true } => default,
            var s => new BigInteger(s, isBigEndian: true)
        };

        /// <summary>
        /// Gets a number (big integer) value.
        /// </summary>
        /// <param name="index">Index.</param>
        /// <returns>Value.</returns>
        public BigInteger? GetNumberNullable(int index) => IsNull(index) ? null : GetNumber(index);

        /// <summary>
        /// Gets a local date value.
        /// </summary>
        /// <param name="index">Index.</param>
        /// <returns>Value.</returns>
        public LocalDate GetDate(int index) => Seek(index) switch
        {
            { IsEmpty: true } => default,
            { Length: 3 } s => ReadDate(s),
            var s => throw GetInvalidLengthException(index, 7, s.Length)
        };

        /// <summary>
        /// Gets a local date value.
        /// </summary>
        /// <param name="index">Index.</param>
        /// <returns>Value.</returns>
        public LocalDate? GetDateNullable(int index) => IsNull(index) ? null : GetDate(index);

        /// <summary>
        /// Gets a local time value.
        /// </summary>
        /// <param name="index">Index.</param>
        /// <returns>Value.</returns>
        public LocalTime GetTime(int index) => Seek(index) switch
        {
            { IsEmpty: true } => default,
            { Length: >= 4 and <= 6 } s => ReadTime(s),
            var s => throw GetInvalidLengthException(index, 6, s.Length)
        };

        /// <summary>
        /// Gets a local time value.
        /// </summary>
        /// <param name="index">Index.</param>
        /// <returns>Value.</returns>
        public LocalTime? GetTimeNullable(int index) => IsNull(index) ? null : GetTime(index);

        /// <summary>
        /// Gets a local date and time value.
        /// </summary>
        /// <param name="index">Index.</param>
        /// <returns>Value.</returns>
        public LocalDateTime GetDateTime(int index) => Seek(index) switch
        {
            { IsEmpty: true } => default,
            { Length: >= 7 and <= 9 } s => ReadDate(s) + ReadTime(s[3..]),
            var s => throw GetInvalidLengthException(index, 9, s.Length)
        };

        /// <summary>
        /// Gets a local date and time value.
        /// </summary>
        /// <param name="index">Index.</param>
        /// <returns>Value.</returns>
        public LocalDateTime? GetDateTimeNullable(int index) => IsNull(index) ? null : GetDateTime(index);

        /// <summary>
        /// Gets a timestamp (instant) value.
        /// </summary>
        /// <param name="index">Index.</param>
        /// <returns>Value.</returns>
        public Instant GetTimestamp(int index) => Seek(index) switch
        {
            { IsEmpty: true } => default,
            { Length: 8 } s => Instant.FromUnixTimeSeconds(BinaryPrimitives.ReadInt64LittleEndian(s)),
            { Length: 12 } s => Instant.FromUnixTimeSeconds(BinaryPrimitives.ReadInt64LittleEndian(s))
                .PlusNanoseconds(BinaryPrimitives.ReadInt32LittleEndian(s[8..])),
            var s => throw GetInvalidLengthException(index, 12, s.Length)
        };

        /// <summary>
        /// Gets a timestamp (instant) value.
        /// </summary>
        /// <param name="index">Index.</param>
        /// <returns>Value.</returns>
        public Instant? GetTimestampNullable(int index) => IsNull(index) ? null : GetTimestamp(index);

        /// <summary>
        /// Gets a duration value.
        /// </summary>
        /// <param name="index">Index.</param>
        /// <returns>Value.</returns>
        public Duration GetDuration(int index) => Seek(index) switch
        {
            { IsEmpty: true } => default,
            { Length: 8 } s => Duration.FromSeconds(BinaryPrimitives.ReadInt64LittleEndian(s)),
            { Length: 12 } s => Duration.FromSeconds(BinaryPrimitives.ReadInt64LittleEndian(s))
                .Plus(Duration.FromNanoseconds(BinaryPrimitives.ReadInt32LittleEndian(s[8..]))),
            var s => throw GetInvalidLengthException(index, 12, s.Length)
        };

        /// <summary>
        /// Gets a duration value.
        /// </summary>
        /// <param name="index">Index.</param>
        /// <returns>Value.</returns>
        public Duration? GetDurationNullable(int index) => IsNull(index) ? null : GetDuration(index);

        /// <summary>
        /// Gets a period value.
        /// </summary>
        /// <param name="index">Index.</param>
        /// <returns>Value.</returns>
        public Period GetPeriod(int index) => Seek(index) switch
        {
            { IsEmpty: true } => Period.Zero,
            { Length: 3 } s => Period.FromYears(unchecked((sbyte)s[0])) +
                               Period.FromMonths(unchecked((sbyte)s[1])) +
                               Period.FromDays(unchecked((sbyte)s[2])),
            { Length: 6 } s => Period.FromYears(BinaryPrimitives.ReadInt16LittleEndian(s)) +
                               Period.FromMonths(BinaryPrimitives.ReadInt16LittleEndian(s[2..])) +
                               Period.FromDays(BinaryPrimitives.ReadInt16LittleEndian(s[4..])),
            { Length: 12 } s => Period.FromYears(BinaryPrimitives.ReadInt32LittleEndian(s)) +
                                Period.FromMonths(BinaryPrimitives.ReadInt32LittleEndian(s[4..])) +
                                Period.FromDays(BinaryPrimitives.ReadInt32LittleEndian(s[8..])),
            var s => throw GetInvalidLengthException(index, 12, s.Length)
        };

        /// <summary>
        /// Gets a period value.
        /// </summary>
        /// <param name="index">Index.</param>
        /// <returns>Value.</returns>
        public Period? GetPeriodNullable(int index) => IsNull(index) ? null : GetPeriod(index);

        /// <summary>
        /// Gets bytes.
        /// </summary>
        /// <param name="index">Index.</param>
        /// <returns>Value.</returns>
        public byte[] GetBytes(int index) => Seek(index) switch
        {
            { IsEmpty: true } => Array.Empty<byte>(),
            var s => s.ToArray()
        };

        /// <summary>
        /// Gets bytes.
        /// </summary>
        /// <param name="index">Index.</param>
        /// <returns>Value.</returns>
        public byte[]? GetBytesNullable(int index) => IsNull(index) ? null : GetBytes(index);

        /// <summary>
        /// Gets bytes.
        /// </summary>
        /// <param name="index">Index.</param>
        /// <returns>Value.</returns>
        public ReadOnlySpan<byte> GetBytesSpan(int index) => Seek(index);

        /// <summary>
        /// Gets an object value according to the specified type.
        /// </summary>
        /// <param name="index">Index.</param>
        /// <param name="columnType">Column type.</param>
        /// <param name="scale">Column decimal scale.</param>
        /// <returns>Value.</returns>
        public object? GetObject(int index, ColumnType columnType, int scale = 0)
        {
            if (IsNull(index))
            {
                return null;
            }

            return columnType switch
            {
                ColumnType.Int8 => GetByte(index),
                ColumnType.Int16 => GetShort(index),
                ColumnType.Int32 => GetInt(index),
                ColumnType.Int64 => GetLong(index),
                ColumnType.Float => GetFloat(index),
                ColumnType.Double => GetDouble(index),
                ColumnType.Uuid => GetGuid(index),
                ColumnType.String => GetString(index),
                ColumnType.Decimal => GetDecimal(index, scale),
                ColumnType.ByteArray => GetBytes(index),
                ColumnType.Bitmask => GetBitmask(index),
                ColumnType.Date => GetDate(index),
                ColumnType.Time => GetTime(index),
                ColumnType.Datetime => GetDateTime(index),
                ColumnType.Timestamp => GetTimestamp(index),
                ColumnType.Number => GetNumber(index),
                ColumnType.Boolean => GetByteAsBool(index),
                ColumnType.Period => GetPeriod(index),
                ColumnType.Duration => GetDuration(index),
                _ => throw new IgniteClientException(ErrorGroups.Client.Protocol, "Unsupported type: " + columnType)
            };
        }

        /// <summary>
        /// Gets an object value according to the type code at the specified index.
        /// </summary>
        /// <param name="index">Index.</param>
        /// <returns>Value.</returns>
        public object? GetObject(int index)
        {
            if (IsNull(index))
            {
                return null;
            }

            var type = (ColumnType)GetInt(index);
            var scale = GetInt(index + 1);

            return GetObject(index + 2, type, scale);
        }

        private static LocalDate ReadDate(ReadOnlySpan<byte> span)
        {
            // Read int32 from 3 bytes, preserving sign.
            Span<byte> buf = stackalloc byte[4];
            span[..3].CopyTo(buf[1..]);

            int date = BinaryPrimitives.ReadInt32LittleEndian(buf) >> 8;

            int day = date & 31;
            int month = (date >> 5) & 15;
            int year = (date >> 9); // Sign matters.

            return new LocalDate(year, month, day);
        }

        private static LocalTime ReadTime(ReadOnlySpan<byte> span)
        {
            long time = BinaryPrimitives.ReadUInt32LittleEndian(span);
            var length = span.Length;

            int nanos;
            if (length == 4)
            {
                nanos = ((int) time & ((1 << 10) - 1)) * 1000 * 1000;
                time >>= 10;
            }
            else if (length == 5)
            {
                time |= (long)span[4] << 32;
                nanos = ((int) time & ((1 << 20) - 1)) * 1000;
                time >>= 20;
            }
            else
            {
                time |= (long)BinaryPrimitives.ReadUInt16LittleEndian(span[4..]) << 32;
                nanos = ((int)time & ((1 << 30) - 1));
                time >>= 30;
            }

            int second = ((int) time) & 63;
            int minute = ((int) time >> 6) & 63;
            int hour = ((int) time >> 12) & 31;

            return LocalTime.FromHourMinuteSecondNanosecond(hour, minute, second, nanos);
        }

        private static decimal ReadDecimal(ReadOnlySpan<byte> span, int scale)
        {
            var unscaled = new BigInteger(span, isBigEndian: true);
            var res = (decimal)unscaled;

            if (scale > 0)
            {
                res /= (decimal)BigInteger.Pow(10, scale);
            }

            return res;
        }

        private static InvalidOperationException GetNullElementException(int index) =>
            new($"Binary tuple element with index {index} is null.");

        private static InvalidOperationException GetInvalidLengthException(int index, int expectedLength, int actualLength) =>
            new($"Binary tuple element with index {index} has invalid length (expected {expectedLength}, actual {actualLength}).");

        private int GetOffset(int position)
        {
            var span = _buffer[position..];

            switch (_entrySize)
            {
                case 1:
                    return span[0];

                case 2:
                    return BinaryPrimitives.ReadUInt16LittleEndian(span);

                case 4:
                {
                    var offset = BinaryPrimitives.ReadInt32LittleEndian(span);

                    if (offset < 0)
                    {
                        throw new InvalidOperationException("Unsupported offset table size");
                    }

                    return offset;
                }

                default:
                    throw new InvalidOperationException("Invalid offset table size");
            }
        }

        private ReadOnlySpan<byte> Seek(int index)
        {
            Debug.Assert(index >= 0, "index >= 0");
            Debug.Assert(index < _numElements, "index < numElements");

            int entry = _entryBase + index * _entrySize;

            int offset = _valueBase;
            if (index > 0)
            {
                offset += GetOffset(entry - _entrySize);
            }

            int nextOffset = _valueBase + GetOffset(entry);

            if (nextOffset < offset)
            {
                throw new InvalidOperationException("Corrupted offset table");
            }

            if (offset == nextOffset && IsNull(index))
            {
                throw GetNullElementException(index);
            }

            return _buffer.Slice(offset, nextOffset - offset);
        }
    }
}
