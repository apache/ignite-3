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
        /// Gets a string value.
        /// </summary>
        /// <param name="index">Index.</param>
        /// <returns>Value.</returns>
        public string GetString(int index) => Seek(index) switch
        {
            { IsEmpty: true } => string.Empty,
            var s => BinaryTupleCommon.StringEncoding.GetString(s)
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
        /// Gets a decimal value.
        /// </summary>
        /// <param name="index">Index.</param>
        /// <param name="scale">Decimal scale.</param>
        /// <returns>Value.</returns>
        public decimal GetDecimal(int index, int scale) => Seek(index) switch
        {
            { IsEmpty: true } => default,
            var s => ReadDecimal2(s, scale)
        };

        /// <summary>
        /// Gets a number (big integer) value.
        /// </summary>
        /// <param name="index">Index.</param>
        /// <returns>Value.</returns>
        public BigInteger GetNumber(int index) => Seek(index) switch
        {
            { IsEmpty: true } => default,
            var s => new BigInteger(s)
        };

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
        /// Gets a timestamp (instant) value.
        /// </summary>
        /// <param name="index">Index.</param>
        /// <returns>Value.</returns>
        public Instant GetTimestamp(int index) => Seek(index) switch
        {
            { IsEmpty: true } => default,
            var s => ReadInstant(s)
        };

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
        /// Gets an object value according to the specified type.
        /// </summary>
        /// <param name="index">Index.</param>
        /// <param name="columnType">Column type.</param>
        /// <param name="scale">Column decimal scale.</param>
        /// <returns>Value.</returns>
        public object? GetObject(int index, ClientDataType columnType, int scale)
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
                ClientDataType.BigInteger => GetNumber(index),
                _ => throw new IgniteClientException(ErrorGroups.Client.Protocol, "Unsupported type: " + columnType)
            };
        }

        private static Instant ReadInstant(ReadOnlySpan<byte> span)
        {
            var len = span.Length;
            if (len == 0)
            {
                return default;
            }

            long seconds = BinaryPrimitives.ReadInt64LittleEndian(span);
            int nanos = len == 8 ? 0 : BinaryPrimitives.ReadInt32LittleEndian(span[8..]);

            return Instant.FromUnixTimeSeconds(seconds).PlusNanoseconds(nanos);
        }

        private static LocalDate ReadDate(ReadOnlySpan<byte> span)
        {
            int date = BinaryPrimitives.ReadUInt16LittleEndian(span);
            date |= span[2..][0] << 16;

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

        private static decimal ReadDecimal2(ReadOnlySpan<byte> span, int scale)
        {
            var unscaled = new BigInteger(span);
            var res = (decimal)unscaled;

            if (scale > 0)
            {
                res /= (decimal)BigInteger.Pow(10, scale);
            }

            return res;
        }

        // ReSharper disable once UnusedMember.Local (TODO IGNITE-15431)
        private static decimal ReadDecimal(ReadOnlySpan<byte> span, int scale)
        {
            Span<byte> mag = stackalloc byte[span.Length];
            span.CopyTo(mag);

            bool negative = false;

            if ((sbyte)mag[0] < 0)
            {
                mag[0] &= 0x7F;

                negative = true;
            }

            if (scale < 0 || scale > 28)
            {
                throw new OverflowException("Decimal value scale overflow (must be between 0 and 28): " + scale);
            }

            if (mag.Length > 13)
            {
                throw new OverflowException("Decimal magnitude overflow (must be less than 96 bits): " + mag.Length * 8);
            }

            if (mag.Length == 13 && mag[0] != 0)
            {
                throw new OverflowException("Decimal magnitude overflow (must be less than 96 bits): " + mag.Length * 8);
            }

            int hi = 0;
            int mid = 0;
            int lo = 0;

            int ctr = -1;

            for (int i = mag.Length - 12; i < mag.Length; i++)
            {
                if (++ctr == 4)
                {
                    mid = lo;
                    lo = 0;
                }
                else if (ctr == 8)
                {
                    hi = mid;
                    mid = lo;
                    lo = 0;
                }

                if (i >= 0)
                {
                    lo = (lo << 8) + mag[i];
                }
            }

            return new decimal(lo, mid, hi, negative, (byte)scale);
        }

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
                throw new InvalidOperationException($"Binary tuple element with index {index} is null.");
            }

            return _buffer.Span.Slice(offset, nextOffset - offset);
        }
    }
}
