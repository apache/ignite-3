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
    using System.Runtime.InteropServices;
    using Buffers;
    using NodaTime;

    /// <summary>
    /// Binary tuple builder.
    /// </summary>
    internal ref struct BinaryTupleBuilder
    {
        /** Number of elements in the tuple. */
        private readonly int _numElements;

        /** Size of an offset table entry. */
        private readonly int _entrySize;

        /** Position of the varlen offset table. */
        private readonly int _entryBase;

        /** Starting position of variable-length values. */
        private readonly int _valueBase;

        /** Buffer for tuple content. */
        private readonly PooledArrayBufferWriter _buffer;

        /** Flag indicating if any NULL values were really put here. */
        private bool _hasNullValues;

        /** Current element. */
        private int _elementIndex;

        /// <summary>
        /// Initializes a new instance of the <see cref="BinaryTupleBuilder"/> struct.
        /// </summary>
        /// <param name="numElements">Capacity.</param>
        /// <param name="allowNulls">Whether nulls are allowed.</param>
        /// <param name="totalValueSize">Total value size, -1 when unknown.</param>
        public BinaryTupleBuilder(int numElements, bool allowNulls = true, int totalValueSize = -1)
        {
            Debug.Assert(numElements >= 0, "numElements >= 0");

            _numElements = numElements;
            _buffer = new();
            _elementIndex = 0;
            _hasNullValues = false;

            int baseOffset = BinaryTupleCommon.HeaderSize;
            if (allowNulls)
            {
                baseOffset += BinaryTupleCommon.NullMapSize(numElements);
            }

            _entryBase = baseOffset;

            _entrySize = totalValueSize < 0
                ? 4
                : BinaryTupleCommon.FlagsToEntrySize(BinaryTupleCommon.ValueSizeToFlags(totalValueSize));

            _valueBase = baseOffset + _entrySize * numElements;

            _buffer.GetSpan(_valueBase);
            _buffer.Advance(_valueBase);
        }

        /// <summary>
        /// Gets a value indicating whether null map is present.
        /// </summary>
        public bool HasNullMap => _entryBase > BinaryTupleCommon.HeaderSize;

        /// <summary>
        /// Gets the current element index.
        /// </summary>
        public int ElementIndex => _elementIndex;

        /// <summary>
        /// Appends a null value.
        /// </summary>
        public void AppendNull()
        {
            if (!HasNullMap)
            {
                throw new InvalidOperationException("Appending a NULL value in binary tuple builder with disabled NULLs");
            }

            _hasNullValues = true;

            int nullIndex = BinaryTupleCommon.NullOffset(_elementIndex);
            byte nullMask = BinaryTupleCommon.NullMask(_elementIndex);

            _buffer.GetSpan(nullIndex, 1)[0] |= nullMask;

            OnWrite();
        }

        /// <summary>
        /// Appends a default value.
        /// </summary>
        public void AppendDefault()
        {
            OnWrite();
        }

        /// <summary>
        /// Appends a byte.
        /// </summary>
        /// <param name="value">Value.</param>
        public void AppendByte(sbyte value)
        {
            if (value != 0)
            {
                PutByte(value);
            }

            OnWrite();
        }

        /// <summary>
        /// Appends a short.
        /// </summary>
        /// <param name="value">Value.</param>
        public void AppendShort(short value)
        {
            if (value >= sbyte.MinValue && value <= sbyte.MaxValue)
            {
                AppendByte((sbyte)value);
            }
            else
            {
                PutShort(value);
                OnWrite();
            }
        }

        /// <summary>
        /// Appends an int.
        /// </summary>
        /// <param name="value">Value.</param>
        public void AppendInt(int value)
        {
            if (value >= sbyte.MinValue && value <= sbyte.MaxValue)
            {
                AppendByte((sbyte)value);
                return;
            }

            if (value >= short.MinValue && value <= short.MaxValue)
            {
                PutShort((short)value);
            }
            else
            {
                PutInt(value);
            }

            OnWrite();
        }

        /// <summary>
        /// Appends a long.
        /// </summary>
        /// <param name="value">Value.</param>
        public void AppendLong(long value)
        {
            if (value >= short.MinValue && value <= short.MaxValue)
            {
                AppendShort((short)value);
                return;
            }

            if (value >= int.MinValue && value <= int.MaxValue)
            {
                PutInt((int)value);
            }
            else
            {
                PutLong(value);
            }

            OnWrite();
        }

        /// <summary>
        /// Appends a gloat.
        /// </summary>
        /// <param name="value">Value.</param>
        public void AppendFloat(float value)
        {
            if (value != 0.0F)
            {
                PutFloat(value);
            }

            OnWrite();
        }

        /// <summary>
        /// Appends a double.
        /// </summary>
        /// <param name="value">Value.</param>
        public void AppendDouble(double value)
        {
            // ReSharper disable once CompareOfFloatsByEqualityOperator
            if (value == (float)value)
            {
                AppendFloat((float)value);
                return;
            }

            PutDouble(value);
            OnWrite();
        }

        /// <summary>
        /// Appends a string.
        /// </summary>
        /// <param name="value">Value.</param>
        public void AppendString(string value)
        {
            PutString(value);

            OnWrite();
        }

        /// <summary>
        /// Appends a string.
        /// </summary>
        /// <param name="value">Value.</param>
        public void AppendStringNullable(string? value)
        {
            if (value == null)
            {
                AppendNull();
                return;
            }

            PutString(value);

            OnWrite();
        }

        /// <summary>
        /// Appends bytes.
        /// </summary>
        /// <param name="value">Value.</param>
        public void AppendBytes(Span<byte> value)
        {
            PutBytes(value);
            OnWrite();
        }

        /// <summary>
        /// Appends bytes.
        /// </summary>
        /// <param name="value">Value.</param>
        public void AppendBytes(byte[] value) => AppendBytes(value.AsSpan());

        /// <summary>
        /// Appends a guid.
        /// </summary>
        /// <param name="value">Value.</param>
        public void AppendGuid(Guid value)
        {
            if (value != default)
            {
                UuidSerializer.WriteLittleEndian(value, GetSpan(16));
            }

            OnWrite();
        }

        /// <summary>
        /// Appends a bitmask.
        /// </summary>
        /// <param name="value">Value.</param>
        public void AppendBitmask(BitArray value)
        {
            var size = (value.Length + 7) / 8; // Ceiling division.
            var arr = ByteArrayPool.Rent(size);

            try
            {
                value.CopyTo(arr, 0);
                PutBytes(arr.AsSpan()[..size]);

                OnWrite();
            }
            finally
            {
                ByteArrayPool.Return(arr);
            }
        }

        /// <summary>
        /// Appends a decimal.
        /// </summary>
        /// <param name="value">Value.</param>
        /// <param name="scale">Decimal scale from schema.</param>
        public void AppendDecimal(decimal value, int scale)
        {
            if (value != decimal.Zero)
            {
                var (unscaledValue, valueScale) = DeconstructDecimal(value);

                PutDecimal(scale, unscaledValue, valueScale);
            }

            OnWrite();
        }

        /// <summary>
        /// Appends a number.
        /// </summary>
        /// <param name="value">Value.</param>
        public void AppendNumber(BigInteger value)
        {
            if (value != default)
            {
                var size = value.GetByteCount();
                var destination = GetSpan(size);
                var success = value.TryWriteBytes(destination, out int written, isBigEndian: true);

                Debug.Assert(success, "success");
                Debug.Assert(written == size, "written == size");
            }

            OnWrite();
        }

        /// <summary>
        /// Appends a date.
        /// </summary>
        /// <param name="value">Value.</param>
        public void AppendDate(LocalDate value)
        {
            if (value != default)
            {
                PutDate(value);
            }

            OnWrite();
        }

        /// <summary>
        /// Appends a time.
        /// </summary>
        /// <param name="value">Value.</param>
        public void AppendTime(LocalTime value)
        {
            if (value != default)
            {
                PutTime(value);
            }

            OnWrite();
        }

        /// <summary>
        /// Appends a date and time.
        /// </summary>
        /// <param name="value">Value.</param>
        public void AppendDateTime(LocalDateTime value)
        {
            if (value != default)
            {
                PutDate(value.Date);
                PutTime(value.TimeOfDay);
            }

            OnWrite();
        }

        /// <summary>
        /// Appends a timestamp (instant).
        /// </summary>
        /// <param name="value">Value.</param>
        public void AppendTimestamp(Instant value)
        {
            if (value != default)
            {
                PutTimestamp(value);
            }

            OnWrite();
        }

        /// <summary>
        /// Appends a duration.
        /// </summary>
        /// <param name="value">Value.</param>
        public void AppendDuration(Duration value)
        {
            if (value != default)
            {
                PutDuration(value);
            }

            OnWrite();
        }

        /// <summary>
        /// Appends a period.
        /// </summary>
        /// <param name="value">Value.</param>
        public void AppendPeriod(Period value)
        {
            if (value != Period.Zero)
            {
                PutPeriod(value);
            }

            OnWrite();
        }

        /// <summary>
        /// Appends an object.
        /// </summary>
        /// <param name="value">Value.</param>
        /// <param name="colType">Column type.</param>
        /// <param name="scale">Decimal scale.</param>
        public void AppendObject(object? value, ClientDataType colType, int scale)
        {
            if (value == null)
            {
                AppendNull();
                return;
            }

            switch (colType)
            {
                case ClientDataType.Int8:
                    AppendByte((sbyte)value);
                    break;

                case ClientDataType.Int16:
                    AppendShort((short)value);
                    break;

                case ClientDataType.Int32:
                    AppendInt((int)value);
                    break;

                case ClientDataType.Int64:
                    AppendLong((long)value);
                    break;

                case ClientDataType.Float:
                    AppendFloat((float)value);
                    break;

                case ClientDataType.Double:
                    AppendDouble((double)value);
                    break;

                case ClientDataType.Uuid:
                    AppendGuid((Guid)value);
                    break;

                case ClientDataType.String:
                    AppendString((string)value);
                    break;

                case ClientDataType.Bytes:
                    AppendBytes((byte[])value);
                    break;

                case ClientDataType.BitMask:
                    AppendBitmask((BitArray)value);
                    break;

                case ClientDataType.Decimal:
                    AppendDecimal((decimal)value, scale);
                    break;

                case ClientDataType.Number:
                    AppendNumber((BigInteger)value);
                    break;

                case ClientDataType.Date:
                    AppendDate((LocalDate)value);
                    break;

                case ClientDataType.Time:
                    AppendTime((LocalTime)value);
                    break;

                case ClientDataType.DateTime:
                    AppendDateTime((LocalDateTime)value);
                    break;

                case ClientDataType.Timestamp:
                    AppendTimestamp((Instant)value);
                    break;

                default:
                    throw new IgniteClientException(ErrorGroups.Client.Protocol, "Unsupported type: " + colType);
            }
        }

        /// <summary>
        /// Appends an object.
        /// </summary>
        /// <param name="value">Value.</param>
        public void AppendObjectWithType(object? value)
        {
            if (value == null)
            {
                AppendNull();
            }
            else if (value is sbyte i8)
            {
                AppendInt((int)ClientDataType.Int8);
                AppendInt(0);
                AppendByte(i8);
            }
            else if (value is short i16)
            {
                AppendInt((int)ClientDataType.Int16);
                AppendInt(0);
                AppendShort(i16);
            }
            else if (value is int i32)
            {
                AppendInt((int)ClientDataType.Int32);
                AppendInt(0);
                AppendInt(i32);
            }
            else if (value is long i64)
            {
                AppendInt((int)ClientDataType.Int64);
                AppendInt(0);
                AppendLong(i64);
            }
            else if (value is float f32)
            {
                AppendInt((int)ClientDataType.Float);
                AppendInt(0);
                AppendFloat(f32);
            }
            else if (value is double f64)
            {
                AppendInt((int)ClientDataType.Double);
                AppendInt(0);
                AppendDouble(f64);
            }
            else if (value is Guid uuid)
            {
                AppendInt((int)ClientDataType.Uuid);
                AppendInt(0);
                AppendGuid(uuid);
            }
            else if (value is string str)
            {
                AppendInt((int)ClientDataType.String);
                AppendInt(0);
                AppendString(str);
            }
            else if (value is byte[] bytes)
            {
                AppendInt((int)ClientDataType.Bytes);
                AppendInt(0);
                AppendBytes(bytes);
            }
            else if (value is BitArray bitArray)
            {
                AppendInt((int)ClientDataType.BitMask);
                AppendInt(0);
                AppendBitmask(bitArray);
            }
            else if (value is decimal dec)
            {
                AppendInt((int)ClientDataType.Decimal);

                var (unscaled, scale) = DeconstructDecimal(dec);

                AppendInt(scale);

                PutDecimal(scale, unscaled, scale);
                OnWrite();
            }
            else if (value is BigInteger bigInt)
            {
                AppendInt((int)ClientDataType.Number);
                AppendInt(0);
                AppendNumber(bigInt);
            }
            else if (value is LocalDate localDate)
            {
                AppendInt((int)ClientDataType.Date);
                AppendInt(0);
                AppendDate(localDate);
            }
            else if (value is LocalTime localTime)
            {
                AppendInt((int)ClientDataType.Time);
                AppendInt(0);
                AppendTime(localTime);
            }
            else if (value is LocalDateTime localDateTime)
            {
                AppendInt((int)ClientDataType.DateTime);
                AppendInt(0);
                AppendDateTime(localDateTime);
            }
            else if (value is Instant instant)
            {
                AppendInt((int)ClientDataType.Timestamp);
                AppendInt(0);
                AppendTimestamp(instant);
            }
            else
            {
                throw new IgniteClientException(ErrorGroups.Client.Protocol, "Unsupported type: " + value.GetType());
            }
        }

        /// <summary>
        /// Builds the tuple.
        /// <para />
        /// NOTE: This should be called only once as it messes up with accumulated internal data.
        /// </summary>
        /// <returns>Resulting memory.</returns>
        public Memory<byte> Build()
        {
            int offset = 0;

            int valueSize = _buffer.Position - _valueBase;
            byte flags = BinaryTupleCommon.ValueSizeToFlags(valueSize);
            int desiredEntrySize = BinaryTupleCommon.FlagsToEntrySize(flags);

            // Shrink the offset table if needed.
            if (desiredEntrySize != _entrySize)
            {
                if (desiredEntrySize > _entrySize)
                {
                    throw new InvalidOperationException("Offset entry overflow in binary tuple builder");
                }

                Debug.Assert(_entrySize == 4 || _entrySize == 2, "_entrySize == 4 || _entrySize == 2");
                Debug.Assert(desiredEntrySize == 2 || desiredEntrySize == 1, "desiredEntrySize == 2 || desiredEntrySize == 1");

                int getIndex = _valueBase;
                int putIndex = _valueBase;
                while (getIndex > _entryBase)
                {
                    getIndex -= _entrySize;
                    putIndex -= desiredEntrySize;

                    var value = _entrySize == 4
                        ? _buffer.ReadInt(getIndex)
                        : _buffer.ReadShort(getIndex);

                    if (desiredEntrySize == 1)
                    {
                        _buffer.WriteByte((byte)value, putIndex);
                    }
                    else
                    {
                        _buffer.WriteShort((short)value, putIndex);
                    }
                }

                offset = (_entrySize - desiredEntrySize) * _numElements;
            }

            // Drop or move null map if needed.
            if (HasNullMap)
            {
                if (!_hasNullValues)
                {
                    offset += BinaryTupleCommon.NullMapSize(_numElements);
                }
                else
                {
                    flags |= BinaryTupleCommon.NullmapFlag;
                    if (offset != 0)
                    {
                        int n = BinaryTupleCommon.NullMapSize(_numElements);
                        for (int i = BinaryTupleCommon.HeaderSize + n - 1; i >= BinaryTupleCommon.HeaderSize; i--)
                        {
                            _buffer.WriteByte(_buffer.ReadByte(i), i + offset);
                        }
                    }
                }
            }

            _buffer.WriteByte(flags, offset);

            return _buffer.GetWrittenMemory().Slice(offset);
        }

        /// <summary>
        /// Disposes this instance.
        /// </summary>
        public void Dispose()
        {
            _buffer.Dispose();
        }

        private static (BigInteger Unscaled, int Scale) DeconstructDecimal(decimal value)
        {
            Span<int> bits = stackalloc int[4];
            decimal.GetBits(value, bits);

            var scale = (bits[3] & 0x00FF0000) >> 16;
            var sign = bits[3] >> 31;

            var bytes = MemoryMarshal.Cast<int, byte>(bits[..3]);
            var unscaled = new BigInteger(bytes, true);

            return (sign < 0 ? -unscaled : unscaled, scale);
        }

        private void PutDecimal(int scale, BigInteger unscaledValue, int valueScale)
        {
            if (scale > valueScale)
            {
                unscaledValue *= BigInteger.Pow(new BigInteger(10), scale - valueScale);
            }
            else if (scale < valueScale)
            {
                unscaledValue /= BigInteger.Pow(new BigInteger(10), valueScale - scale);
            }

            var size = unscaledValue.GetByteCount();
            var destination = GetSpan(size);
            var success = unscaledValue.TryWriteBytes(destination, out int written, isBigEndian: true);

            Debug.Assert(success, "success");
            Debug.Assert(written == size, "written == size");
        }

        private void PutByte(sbyte value) => _buffer.WriteByte(unchecked((byte)value));

        private void PutShort(short value) => _buffer.WriteShort(value);

        private void PutInt(int value) => _buffer.WriteInt(value);

        private void PutLong(long value) => _buffer.WriteLong(value);

        private void PutFloat(float value) => PutInt(BitConverter.SingleToInt32Bits(value));

        private void PutDouble(double value) => PutLong(BitConverter.DoubleToInt64Bits(value));

        private void PutBytes(Span<byte> bytes) => bytes.CopyTo(GetSpan(bytes.Length));

        private void PutString(string value)
        {
            if (value.Length == 0)
            {
                return;
            }

            var maxByteCount = BinaryTupleCommon.StringEncoding.GetMaxByteCount(value.Length);
            var span = _buffer.GetSpan(maxByteCount);

            var actualBytes = BinaryTupleCommon.StringEncoding.GetBytes(value, span);

            _buffer.Advance(actualBytes);
        }

        private void PutTimestamp(Instant value)
        {
            // Logic taken from
            // https://github.com/nodatime/nodatime.serialization/blob/main/src/NodaTime.Serialization.Protobuf/NodaExtensions.cs#L69
            // (Apache License).
            // See discussion: https://github.com/nodatime/nodatime/issues/1644#issuecomment-1260524451
            long seconds = value.ToUnixTimeSeconds();
            Duration remainder = value - Instant.FromUnixTimeSeconds(seconds);
            int nanos = (int)remainder.NanosecondOfDay;

            PutLong(seconds);

            if (nanos != 0)
            {
                PutInt(nanos);
            }
        }

        private void PutDuration(Duration value)
        {
            // Logic taken from
            // https://github.com/nodatime/nodatime.serialization/blob/main/src/NodaTime.Serialization.Protobuf/NodaExtensions.cs#L42
            // (Apache License).
            long days = value.Days;
            long nanoOfDay = value.NanosecondOfDay;
            long secondOfDay = nanoOfDay / NodaConstants.NanosecondsPerSecond;
            long seconds = days * NodaConstants.SecondsPerDay + secondOfDay;
            int nanos = value.SubsecondNanoseconds;

            PutLong(seconds);

            if (nanos != 0)
            {
                PutInt(nanos);
            }
        }

        private void PutPeriod(Period value)
        {
            if (value.HasTimeComponent)
            {
                throw new NotSupportedException("Period with time component is not supported.");
            }

            if (value.Weeks != 0)
            {
                throw new NotSupportedException("Period with weeks component is not supported.");
            }

            int years = value.Years;
            int months = value.Months;
            int days = value.Days;

            if (years is >= sbyte.MinValue and <= sbyte.MaxValue &&
                months is >= sbyte.MinValue and <= sbyte.MaxValue &&
                days is >= sbyte.MinValue and <= sbyte.MaxValue)
            {
                PutByte((sbyte) years);
                PutByte((sbyte) months);
                PutByte((sbyte) days);
            }
            else if (years is >= short.MinValue and <= short.MaxValue &&
                     months is >= short.MinValue and <= short.MaxValue &&
                     days is >= short.MinValue and <= short.MaxValue)
            {
                PutShort((short) years);
                PutShort((short) months);
                PutShort((short) days);
            }
            else
            {
                PutInt(years);
                PutInt(months);
                PutInt(days);
            }
        }

        private void PutTime(LocalTime value)
        {
            long hour = value.Hour;
            long minute = value.Minute;
            long second = value.Second;
            long nanos = value.NanosecondOfSecond;

            if ((nanos % 1000) != 0)
            {
                long time = (hour << 42) | (minute << 36) | (second << 30) | nanos;
                PutInt((int)time);
                PutShort((short)(time >> 32));
            }
            else if ((nanos % 1000000) != 0)
            {
                long time = (hour << 32) | (minute << 26) | (second << 20) | (nanos / 1000);
                PutInt((int)time);
                PutByte((sbyte)(time >> 32));
            }
            else
            {
                long time = (hour << 22) | (minute << 16) | (second << 10) | (nanos / 1000000);
                PutInt((int)time);
            }
        }

        private void PutDate(LocalDate value)
        {
            int year = value.Year;
            int month = value.Month;
            int day = value.Day;

            int date = (year << 9) | (month << 5) | day;

            // Write int32 as 3 bytes, preserving sign.
            Span<byte> buf = stackalloc byte[4];
            BinaryPrimitives.WriteInt32LittleEndian(buf, date << 8);

            buf[1..].CopyTo(GetSpan(3));
        }

        /// <summary>
        /// Proceed to the next tuple element.
        /// </summary>
        private void OnWrite()
        {
            Debug.Assert(_elementIndex < _numElements, "_elementIndex < _numElements");

            int offset = _buffer.Position - _valueBase;

            switch (_entrySize)
            {
                case 1:
                    _buffer.WriteByte((byte)offset, _entryBase + _elementIndex);
                    break;

                case 2:
                    _buffer.WriteShort((short)offset, _entryBase + _elementIndex * 2);
                    break;

                case 4:
                    _buffer.WriteInt(offset, _entryBase + _elementIndex * 4);
                    break;

                default:
                    throw new InvalidOperationException("Tuple entry size is invalid.");
            }

            _elementIndex++;
        }

        private Span<byte> GetSpan(int size)
        {
            var span = _buffer.GetSpan(size);

            _buffer.Advance(size);

            return span;
        }
    }
}
