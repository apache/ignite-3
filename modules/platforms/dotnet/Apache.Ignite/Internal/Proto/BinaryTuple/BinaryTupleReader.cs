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
    using NodaTime;

    /// <summary>
    /// Binary tuple reader.
    /// </summary>
    internal readonly ref struct BinaryTupleReader
    {
        /** Buffer. */
        private readonly ReadOnlyMemory<byte> _buffer;

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
        public BinaryTupleReader(ReadOnlyMemory<byte> buffer, int numElements)
        {
            _buffer = buffer;
            _numElements = numElements;

            var flags = buffer.Span[0];

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

            return (_buffer.Span[nullIndex] & nullMask) != 0;
        }

        /// <summary>
        /// Gets a byte value.
        /// </summary>
        /// <param name="index">Index.</param>
        /// <returns>Value.</returns>
        public sbyte GetByte(int index) => Seek(index) switch
        {
            { IsEmpty: true } => default,
            var s => unchecked((sbyte)s[0])
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
            var s => BinaryPrimitives.ReadInt16LittleEndian(s)
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
            var s => BinaryPrimitives.ReadInt32LittleEndian(s)
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
            var s => BinaryPrimitives.ReadInt64LittleEndian(s)
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
            var s => UuidSerializer.Read(s)
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
            var s => BitConverter.Int32BitsToSingle(BinaryPrimitives.ReadInt32LittleEndian(s))
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
            var s => BitConverter.Int64BitsToDouble(BinaryPrimitives.ReadInt64LittleEndian(s))
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
            var s => ReadDate(s)
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
            var s => ReadTime(s)
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
            var s => ReadDate(s) + ReadTime(s[3..])
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
            var s => Instant
                .FromUnixTimeSeconds(BinaryPrimitives.ReadInt64LittleEndian(s))
                .PlusNanoseconds(s.Length == 8 ? 0 : BinaryPrimitives.ReadInt32LittleEndian(s[8..]))
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
            var s => Duration
                .FromSeconds(BinaryPrimitives.ReadInt64LittleEndian(s))
                .Plus(Duration.FromNanoseconds(s.Length == 8 ? 0 : BinaryPrimitives.ReadInt32LittleEndian(s[8..])))
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
            var s => Period.FromYears(BinaryPrimitives.ReadInt32LittleEndian(s)) +
                     Period.FromMonths(BinaryPrimitives.ReadInt32LittleEndian(s[4..])) +
                     Period.FromDays(BinaryPrimitives.ReadInt32LittleEndian(s[8..]))
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
        /// Gets an object value according to the specified type.
        /// </summary>
        /// <param name="index">Index.</param>
        /// <param name="columnType">Column type.</param>
        /// <param name="scale">Column decimal scale.</param>
        /// <returns>Value.</returns>
        public object? GetObject(int index, ClientDataType columnType, int scale = 0)
        {
            if (IsNull(index))
            {
                return null;
            }

            return columnType switch
            {
                ClientDataType.Int8 => GetByte(index),
                ClientDataType.Int16 => GetShort(index),
                ClientDataType.Int32 => GetInt(index),
                ClientDataType.Int64 => GetLong(index),
                ClientDataType.Float => GetFloat(index),
                ClientDataType.Double => GetDouble(index),
                ClientDataType.Uuid => GetGuid(index),
                ClientDataType.String => GetString(index),
                ClientDataType.Decimal => GetDecimal(index, scale),
                ClientDataType.Bytes => GetBytes(index),
                ClientDataType.BitMask => GetBitmask(index),
                ClientDataType.Date => GetDate(index),
                ClientDataType.Time => GetTime(index),
                ClientDataType.DateTime => GetDateTime(index),
                ClientDataType.Timestamp => GetTimestamp(index),
                ClientDataType.Number => GetNumber(index),
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

            var type = (ClientDataType)GetInt(index);
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

        private int GetOffset(int position)
        {
            var span = _buffer.Span[position..];

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

            return _buffer.Span.Slice(offset, nextOffset - offset);
        }
    }
}
