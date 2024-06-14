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
    using System.Buffers;
    using System.Buffers.Binary;
    using System.Collections;
    using System.Diagnostics;
    using System.Diagnostics.CodeAnalysis;
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

            _entryBase = BinaryTupleCommon.HeaderSize;
            _entrySize = 1 << (flags & BinaryTupleCommon.VarsizeMask);
            _valueBase = _entryBase + _entrySize * numElements;
        }

        /// <summary>
        /// Gets a value indicating whether the element at specified index is null.
        /// </summary>
        /// <param name="index">Element index.</param>
        /// <returns>True when the element is null; false otherwise.</returns>
        public bool IsNull(int index) => Seek(index).IsEmpty;

        /// <summary>
        /// Gets a byte value.
        /// </summary>
        /// <param name="index">Index.</param>
        /// <returns>Value.</returns>
        public sbyte GetByte(int index) => GetByteNullable(index) ?? ThrowNullElementException<sbyte>(index);

        /// <summary>
        /// Gets a byte value.
        /// </summary>
        /// <param name="index">Index.</param>
        /// <returns>Value.</returns>
        public sbyte? GetByteNullable(int index) => Seek(index) switch
        {
            { IsEmpty: true } => null,
            { Length: 1 } s => unchecked((sbyte)s[0]),
            var s => throw GetInvalidLengthException(index, 1, s.Length)
        };

        /// <summary>
        /// Gets a byte value as bool.
        /// </summary>
        /// <param name="index">Index.</param>
        /// <returns>Value.</returns>
        public bool GetBool(int index) => GetBoolNullable(index) ?? ThrowNullElementException<bool>(index);

        /// <summary>
        /// Gets a byte value as bool.
        /// </summary>
        /// <param name="index">Index.</param>
        /// <returns>Value.</returns>
        public bool? GetBoolNullable(int index) => GetByteNullable(index) switch
        {
            null => null,
            { } b => BinaryTupleCommon.ByteToBool(b)
        };

        /// <summary>
        /// Gets a short value.
        /// </summary>
        /// <param name="index">Index.</param>
        /// <returns>Value.</returns>
        public short GetShort(int index) => GetShortNullable(index) ?? ThrowNullElementException<short>(index);

        /// <summary>
        /// Gets a short value.
        /// </summary>
        /// <param name="index">Index.</param>
        /// <returns>Value.</returns>
        public short? GetShortNullable(int index) => Seek(index) switch
        {
            { IsEmpty: true } => null,
            { Length: 1 } s => unchecked((sbyte)s[0]),
            { Length: 2 } s => BinaryPrimitives.ReadInt16LittleEndian(s),
            var s => throw GetInvalidLengthException(index, 2, s.Length)
        };

        /// <summary>
        /// Gets an int value.
        /// </summary>
        /// <param name="index">Index.</param>
        /// <returns>Value.</returns>
        public int GetInt(int index) => GetIntNullable(index) ?? ThrowNullElementException<int>(index);

        /// <summary>
        /// Gets an int value.
        /// </summary>
        /// <param name="index">Index.</param>
        /// <returns>Value.</returns>
        public int? GetIntNullable(int index) => Seek(index) switch
        {
            { IsEmpty: true } => null,
            { Length: 1 } s => unchecked((sbyte)s[0]),
            { Length: 2 } s => BinaryPrimitives.ReadInt16LittleEndian(s),
            { Length: 4 } s => BinaryPrimitives.ReadInt32LittleEndian(s),
            var s => throw GetInvalidLengthException(index, 4, s.Length)
        };

        /// <summary>
        /// Gets a long value.
        /// </summary>
        /// <param name="index">Index.</param>
        /// <returns>Value.</returns>
        public long GetLong(int index) => GetLongNullable(index) ?? ThrowNullElementException<long>(index);

        /// <summary>
        /// Gets a long value.
        /// </summary>
        /// <param name="index">Index.</param>
        /// <returns>Value.</returns>
        public long? GetLongNullable(int index) => Seek(index) switch
        {
            { IsEmpty: true } => null,
            { Length: 1 } s => unchecked((sbyte)s[0]),
            { Length: 2 } s => BinaryPrimitives.ReadInt16LittleEndian(s),
            { Length: 4 } s => BinaryPrimitives.ReadInt32LittleEndian(s),
            { Length: 8 } s => BinaryPrimitives.ReadInt64LittleEndian(s),
            var s => throw GetInvalidLengthException(index, 8, s.Length)
        };

        /// <summary>
        /// Gets a Guid value.
        /// </summary>
        /// <param name="index">Index.</param>
        /// <returns>Value.</returns>
        public Guid GetGuid(int index) => GetGuidNullable(index) ?? ThrowNullElementException<Guid>(index);

        /// <summary>
        /// Gets a Guid value.
        /// </summary>
        /// <param name="index">Index.</param>
        /// <returns>Value.</returns>
        public Guid? GetGuidNullable(int index) => Seek(index) switch
        {
            { IsEmpty: true } => null,
            { Length: 16 } s => UuidSerializer.Read(s),
            var s => throw GetInvalidLengthException(index, 16, s.Length)
        };

        /// <summary>
        /// Gets a string value.
        /// </summary>
        /// <param name="index">Index.</param>
        /// <returns>Value.</returns>
        public string GetString(int index) => GetStringNullable(index) ?? ThrowNullElementException<string>(index);

        /// <summary>
        /// Gets a string value.
        /// </summary>
        /// <param name="index">Index.</param>
        /// <returns>Value.</returns>
        public string? GetStringNullable(int index) => Seek(index) switch
        {
            { IsEmpty: true } => null,
            { Length: 1 } s when s[0] == BinaryTupleCommon.VarlenEmptyByte => string.Empty,
            var s => ProtoCommon.StringEncoding.GetString(s)
        };

        /// <summary>
        /// Gets a float value.
        /// </summary>
        /// <param name="index">Index.</param>
        /// <returns>Value.</returns>
        public float GetFloat(int index) => GetFloatNullable(index) ?? ThrowNullElementException<float>(index);

        /// <summary>
        /// Gets a float value.
        /// </summary>
        /// <param name="index">Index.</param>
        /// <returns>Value.</returns>
        public float? GetFloatNullable(int index) => Seek(index) switch
        {
            { IsEmpty: true } => null,
            { Length: 4 } s => BitConverter.Int32BitsToSingle(BinaryPrimitives.ReadInt32LittleEndian(s)),
            var s => throw GetInvalidLengthException(index, 4, s.Length)
        };

        /// <summary>
        /// Gets a double value.
        /// </summary>
        /// <param name="index">Index.</param>
        /// <returns>Value.</returns>
        public double GetDouble(int index) => GetDoubleNullable(index) ?? ThrowNullElementException<double>(index);

        /// <summary>
        /// Gets a double value.
        /// </summary>
        /// <param name="index">Index.</param>
        /// <returns>Value.</returns>
        public double? GetDoubleNullable(int index) => Seek(index) switch
        {
            { IsEmpty: true } => null,
            { Length: 4 } s => BitConverter.Int32BitsToSingle(BinaryPrimitives.ReadInt32LittleEndian(s)),
            { Length: 8 } s => BitConverter.Int64BitsToDouble(BinaryPrimitives.ReadInt64LittleEndian(s)),
            var s => throw GetInvalidLengthException(index, 8, s.Length)
        };

        /// <summary>
        /// Gets a bit mask value.
        /// </summary>
        /// <param name="index">Index.</param>
        /// <returns>Value.</returns>
        public BitArray GetBitmask(int index) => new(GetBytes(index));

        /// <summary>
        /// Gets a bit mask value.
        /// </summary>
        /// <param name="index">Index.</param>
        /// <returns>Value.</returns>
        public BitArray? GetBitmaskNullable(int index) => GetBytesNullable(index) switch
        {
            null => null,
            var bytes => new BitArray(bytes)
        };

        /// <summary>
        /// Gets a decimal value.
        /// </summary>
        /// <param name="index">Index.</param>
        /// <param name="scale">Decimal scale.</param>
        /// <returns>Value.</returns>
        public decimal GetDecimal(int index, int scale) => GetDecimalNullable(index, scale) ?? ThrowNullElementException<decimal>(index);

        /// <summary>
        /// Gets a decimal value.
        /// </summary>
        /// <param name="index">Index.</param>
        /// <param name="scale">Decimal scale.</param>
        /// <returns>Value.</returns>
        public decimal? GetDecimalNullable(int index, int scale) => ReadDecimal(Seek(index), scale);

        /// <summary>
        /// Gets a number (big integer) value.
        /// </summary>
        /// <param name="index">Index.</param>
        /// <returns>Value.</returns>
        public BigInteger GetNumber(int index) => GetNumberNullable(index) ?? ThrowNullElementException<BigInteger>(index);

        /// <summary>
        /// Gets a number (big integer) value.
        /// </summary>
        /// <param name="index">Index.</param>
        /// <returns>Value.</returns>
        public BigInteger? GetNumberNullable(int index) => Seek(index) switch
        {
            { IsEmpty: true } => null,
            var s => new BigInteger(s, isBigEndian: true)
        };

        /// <summary>
        /// Gets a local date value.
        /// </summary>
        /// <param name="index">Index.</param>
        /// <returns>Value.</returns>
        public LocalDate GetDate(int index) => GetDateNullable(index) ?? ThrowNullElementException<LocalDate>(index);

        /// <summary>
        /// Gets a local date value.
        /// </summary>
        /// <param name="index">Index.</param>
        /// <returns>Value.</returns>
        public LocalDate? GetDateNullable(int index) => Seek(index) switch
        {
            { IsEmpty: true } => null,
            { Length: 3 } s => ReadDate(s),
            var s => throw GetInvalidLengthException(index, 3, s.Length)
        };

        /// <summary>
        /// Gets a local time value.
        /// </summary>
        /// <param name="index">Index.</param>
        /// <returns>Value.</returns>
        public LocalTime GetTime(int index) => GetTimeNullable(index) ?? ThrowNullElementException<LocalTime>(index);

        /// <summary>
        /// Gets a local time value.
        /// </summary>
        /// <param name="index">Index.</param>
        /// <returns>Value.</returns>
        public LocalTime? GetTimeNullable(int index) => Seek(index) switch
        {
            { IsEmpty: true } => null,
            { Length: >= 4 and <= 6 } s => ReadTime(s),
            var s => throw GetInvalidLengthException(index, 6, s.Length)
        };

        /// <summary>
        /// Gets a local date and time value.
        /// </summary>
        /// <param name="index">Index.</param>
        /// <returns>Value.</returns>
        public LocalDateTime GetDateTime(int index) => GetDateTimeNullable(index) ?? ThrowNullElementException<LocalDateTime>(index);

        /// <summary>
        /// Gets a local date and time value.
        /// </summary>
        /// <param name="index">Index.</param>
        /// <returns>Value.</returns>
        public LocalDateTime? GetDateTimeNullable(int index) => Seek(index) switch
        {
            { IsEmpty: true } => null,
            { Length: >= 7 and <= 9 } s => ReadDate(s) + ReadTime(s[3..]),
            var s => throw GetInvalidLengthException(index, 9, s.Length)
        };

        /// <summary>
        /// Gets a timestamp (instant) value.
        /// </summary>
        /// <param name="index">Index.</param>
        /// <returns>Value.</returns>
        public Instant GetTimestamp(int index) => GetTimestampNullable(index) ?? ThrowNullElementException<Instant>(index);

        /// <summary>
        /// Gets a timestamp (instant) value.
        /// </summary>
        /// <param name="index">Index.</param>
        /// <returns>Value.</returns>
        public Instant? GetTimestampNullable(int index) => Seek(index) switch
        {
            { IsEmpty: true } => null,
            { Length: 8 } s => Instant.FromUnixTimeSeconds(BinaryPrimitives.ReadInt64LittleEndian(s)),
            { Length: 12 } s => Instant.FromUnixTimeSeconds(BinaryPrimitives.ReadInt64LittleEndian(s))
                .PlusNanoseconds(BinaryPrimitives.ReadInt32LittleEndian(s[8..])),
            var s => throw GetInvalidLengthException(index, 12, s.Length)
        };

        /// <summary>
        /// Gets a duration value.
        /// </summary>
        /// <param name="index">Index.</param>
        /// <returns>Value.</returns>
        public Duration GetDuration(int index) => GetDurationNullable(index) ?? ThrowNullElementException<Duration>(index);

        /// <summary>
        /// Gets a duration value.
        /// </summary>
        /// <param name="index">Index.</param>
        /// <returns>Value.</returns>
        public Duration? GetDurationNullable(int index) => Seek(index) switch
        {
            { IsEmpty: true } => null,
            { Length: 8 } s => Duration.FromSeconds(BinaryPrimitives.ReadInt64LittleEndian(s)),
            { Length: 12 } s => Duration.FromSeconds(BinaryPrimitives.ReadInt64LittleEndian(s))
                .Plus(Duration.FromNanoseconds(BinaryPrimitives.ReadInt32LittleEndian(s[8..]))),
            var s => throw GetInvalidLengthException(index, 12, s.Length)
        };

        /// <summary>
        /// Gets a period value.
        /// </summary>
        /// <param name="index">Index.</param>
        /// <returns>Value.</returns>
        public Period GetPeriod(int index) => GetPeriodNullable(index) ?? ThrowNullElementException<Period>(index);

        /// <summary>
        /// Gets a period value.
        /// </summary>
        /// <param name="index">Index.</param>
        /// <returns>Value.</returns>
        public Period? GetPeriodNullable(int index) => Seek(index) switch
        {
            { IsEmpty: true } => null,
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
        /// Gets bytes.
        /// </summary>
        /// <param name="index">Index.</param>
        /// <returns>Value.</returns>
        public byte[] GetBytes(int index) => GetBytesNullable(index) ?? throw GetNullElementException(index);

        /// <summary>
        /// Gets bytes.
        /// </summary>
        /// <param name="index">Index.</param>
        /// <returns>Value.</returns>
        public byte[]? GetBytesNullable(int index) => Seek(index) switch
        {
            { IsEmpty: true } => null,
            var s when s[0] == BinaryTupleCommon.VarlenEmptyByte => s[1..].ToArray(),
            var s => s.ToArray()
        };

        /// <summary>
        /// Gets bytes.
        /// </summary>
        /// <param name="index">Index.</param>
        /// <returns>Value.</returns>
        public ReadOnlySpan<byte> GetBytesSpan(int index) => Seek(index) switch
        {
            { IsEmpty: true } => throw GetNullElementException(index),
            var s when s[0] == BinaryTupleCommon.VarlenEmptyByte => s[1..],
            var s => s
        };

        /// <summary>
        /// Gets an object value according to the specified type.
        /// </summary>
        /// <param name="index">Index.</param>
        /// <param name="columnType">Column type.</param>
        /// <param name="scale">Column decimal scale.</param>
        /// <returns>Value.</returns>
        public object? GetObject(int index, ColumnType columnType, int scale = 0) =>
            columnType switch
            {
                ColumnType.Null => null,
                ColumnType.Int8 => GetByteNullable(index),
                ColumnType.Int16 => GetShortNullable(index),
                ColumnType.Int32 => GetIntNullable(index),
                ColumnType.Int64 => GetLongNullable(index),
                ColumnType.Float => GetFloatNullable(index),
                ColumnType.Double => GetDoubleNullable(index),
                ColumnType.Uuid => GetGuidNullable(index),
                ColumnType.String => GetStringNullable(index),
                ColumnType.Decimal => GetDecimalNullable(index, scale),
                ColumnType.ByteArray => GetBytesNullable(index),
                ColumnType.Bitmask => GetBitmaskNullable(index),
                ColumnType.Date => GetDateNullable(index),
                ColumnType.Time => GetTimeNullable(index),
                ColumnType.Datetime => GetDateTimeNullable(index),
                ColumnType.Timestamp => GetTimestampNullable(index),
                ColumnType.Number => GetNumberNullable(index),
                ColumnType.Boolean => GetBoolNullable(index),
                ColumnType.Period => GetPeriodNullable(index),
                ColumnType.Duration => GetDurationNullable(index),
                _ => throw new IgniteClientException(ErrorGroups.Client.Protocol, "Unsupported type: " + columnType)
            };

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

        /// <summary>
        /// Gets an object collection with the specified element type.
        /// Opposite of <see cref="BinaryTupleBuilder.AppendObjectCollectionWithType{T}"/>.
        /// </summary>
        /// <typeparam name="T">Element type.</typeparam>
        /// <returns>Pooled array with items and actual item count.</returns>
        public (T[] Items, int Count) GetObjectCollectionWithType<T>(int startIndex)
        {
            int typeId = GetInt(startIndex++);
            int count = GetInt(startIndex++);

            if (count == 0)
            {
                return (Array.Empty<T>(), 0);
            }

            ColumnType type = (ColumnType)typeId;

            switch (type)
            {
                case ColumnType.Boolean:
                {
                    var items = ArrayPool<bool>.Shared.Rent(count);

                    for (int i = 0; i < count; i++)
                    {
                        items[i] = GetBool(startIndex++);
                    }

                    return ((T[])(object)items, count);
                }
                case ColumnType.Null:
                    break;
                case ColumnType.Int8:
                    break;
                case ColumnType.Int16:
                    break;
                case ColumnType.Int32:
                    break;
                case ColumnType.Int64:
                    break;
                case ColumnType.Float:
                    break;
                case ColumnType.Double:
                    break;
                case ColumnType.Decimal:
                    break;
                case ColumnType.Date:
                    break;
                case ColumnType.Time:
                    break;
                case ColumnType.Datetime:
                    break;
                case ColumnType.Timestamp:
                    break;
                case ColumnType.Uuid:
                    break;
                case ColumnType.Bitmask:
                    break;
                case ColumnType.String:
                    break;
                case ColumnType.ByteArray:
                    break;
                case ColumnType.Period:
                    break;
                case ColumnType.Duration:
                    break;
                case ColumnType.Number:
                    break;
                default:
                    throw new IgniteClientException(ErrorGroups.Client.Protocol, "Unsupported type: " + typeId);
            }
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

        [SuppressMessage("ReSharper", "UnusedParameter.Local", Justification = "Schema scale is not required for deserialization.")]
        private static decimal? ReadDecimal(ReadOnlySpan<byte> span, int scale)
        {
            if (span.IsEmpty)
            {
                return null;
            }

            var valScale = BinaryPrimitives.ReadInt16LittleEndian(span[..2]);
            return ReadDecimalUnscaled(span[2..], valScale);
        }

        private static decimal? ReadDecimalUnscaled(ReadOnlySpan<byte> span, int scale)
        {
            var unscaled = new BigInteger(span, isBigEndian: true);
            var res = (decimal)unscaled;

            if (scale > 0)
            {
                res /= (decimal)BigInteger.Pow(10, scale);
            }
            else if (scale < 0)
            {
                res *= (decimal)BigInteger.Pow(10, -scale);
            }

            return res;
        }

        private static T ThrowNullElementException<T>(int index) => throw GetNullElementException(index);

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

            return _buffer.Slice(offset, nextOffset - offset);
        }
    }
}
