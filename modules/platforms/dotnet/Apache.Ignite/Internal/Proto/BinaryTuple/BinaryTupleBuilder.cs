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
    using Ignite.Sql;
    using NodaTime;
    using Table;

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

        /** Colocation column index provider. When not null, used to compute colocation hash on the fly. */
        private readonly IHashedColumnIndexProvider? _hashedColumnsPredicate;

        /** Buffer for tuple content. */
        private readonly PooledArrayBuffer _buffer;

        /** Current element. */
        private int _elementIndex;

        /// <summary>
        /// Initializes a new instance of the <see cref="BinaryTupleBuilder"/> struct.
        /// </summary>
        /// <param name="numElements">Capacity.</param>
        /// <param name="totalValueSize">Total value size, -1 when unknown.</param>
        /// <param name="hashedColumnsPredicate">A predicate that returns true for colocation column indexes.
        /// Pass null when colocation hash is not needed.</param>
        public BinaryTupleBuilder(
            int numElements,
            int totalValueSize = -1,
            IHashedColumnIndexProvider? hashedColumnsPredicate = null)
        {
            Debug.Assert(numElements >= 0, "numElements >= 0");

            _numElements = numElements;
            _hashedColumnsPredicate = hashedColumnsPredicate;
            _buffer = new();
            _elementIndex = 0;

            // Reserve buffer for individual hash codes.
            _entryBase = _hashedColumnsPredicate != null
                ? BinaryTupleCommon.HeaderSize + _hashedColumnsPredicate.HashedColumnCount * 4
                : BinaryTupleCommon.HeaderSize;

            _entrySize = totalValueSize < 0
                ? 4
                : BinaryTupleCommon.FlagsToEntrySize(BinaryTupleCommon.ValueSizeToFlags(totalValueSize));

            _valueBase = _entryBase + _entrySize * numElements;

            _buffer.GetSpan(size: _valueBase)[.._valueBase].Clear();
            _buffer.Advance(_valueBase);
        }

        /// <summary>
        /// Gets the current element index.
        /// </summary>
        public int ElementIndex => _elementIndex;

        /// <summary>
        /// Gets the hash from column values according to specified <see cref="IHashedColumnIndexProvider"/>.
        /// </summary>
        /// <returns>Column hash according to specified <see cref="IHashedColumnIndexProvider"/>.</returns>
        public int GetHash()
        {
            if (_hashedColumnsPredicate == null)
            {
                return 0;
            }

            var hash = 0;
            var hashes = GetHashSpan();

            for (var i = 0; i < _hashedColumnsPredicate.HashedColumnCount; i++)
            {
                var colHash = hashes[i];
                hash = HashUtils.Combine(hash, colHash);
            }

            return hash;
        }

        /// <summary>
        /// Appends a null value.
        /// </summary>
        public void AppendNull()
        {
            if (GetHashOrder() is { } hashOrder)
            {
                PutHash(hashOrder, HashUtils.Hash32((sbyte)0));
            }

            OnWrite();
        }

        /// <summary>
        /// Appends a byte.
        /// </summary>
        /// <param name="value">Value.</param>
        public void AppendByte(sbyte value)
        {
            if (GetHashOrder() is { } hashOrder)
            {
                PutHash(hashOrder, HashUtils.Hash32(value));
            }

            PutByte(value);
            OnWrite();
        }

        /// <summary>
        /// Appends a byte.
        /// </summary>
        /// <param name="value">Value.</param>
        public void AppendByteNullable(sbyte? value)
        {
            if (value == null)
            {
                AppendNull();
            }
            else
            {
                AppendByte(value.Value);
            }
        }

        /// <summary>
        /// Appends a bool.
        /// </summary>
        /// <param name="value">Value.</param>
        public void AppendBool(bool value)
        {
            var v = BinaryTupleCommon.BoolToByte(value);

            if (GetHashOrder() is { } hashOrder)
            {
                PutHash(hashOrder, HashUtils.Hash32(v));
            }

            PutByte(v);
            OnWrite();
        }

        /// <summary>
        /// Appends a bool.
        /// </summary>
        /// <param name="value">Value.</param>
        public void AppendBoolNullable(bool? value)
        {
            if (value == null)
            {
                AppendNull();
            }
            else
            {
                AppendBool(value.Value);
            }
        }

        /// <summary>
        /// Appends a short.
        /// </summary>
        /// <param name="value">Value.</param>
        public void AppendShort(short value)
        {
            if (GetHashOrder() is { } hashOrder)
            {
                PutHash(hashOrder, HashUtils.Hash32(value));
            }

            if (value >= sbyte.MinValue && value <= sbyte.MaxValue)
            {
                PutByte((sbyte)value);
            }
            else
            {
                PutShort(value);
            }

            OnWrite();
        }

        /// <summary>
        /// Appends a short.
        /// </summary>
        /// <param name="value">Value.</param>
        public void AppendShortNullable(short? value)
        {
            if (value == null)
            {
                AppendNull();
            }
            else
            {
                AppendShort(value.Value);
            }
        }

        /// <summary>
        /// Appends an int.
        /// </summary>
        /// <param name="value">Value.</param>
        public void AppendInt(int value)
        {
            if (GetHashOrder() is { } hashOrder)
            {
                PutHash(hashOrder, HashUtils.Hash32(value));
            }

            if (value >= sbyte.MinValue && value <= sbyte.MaxValue)
            {
                PutByte((sbyte)value);
            }
            else if (value >= short.MinValue && value <= short.MaxValue)
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
        /// Appends an int.
        /// </summary>
        /// <param name="value">Value.</param>
        public void AppendIntNullable(int? value)
        {
            if (value == null)
            {
                AppendNull();
            }
            else
            {
                AppendInt(value.Value);
            }
        }

        /// <summary>
        /// Appends a long.
        /// </summary>
        /// <param name="value">Value.</param>
        public void AppendLong(long value)
        {
            if (GetHashOrder() is { } hashOrder)
            {
                PutHash(hashOrder, HashUtils.Hash32(value));
            }

            if (value >= sbyte.MinValue && value <= sbyte.MaxValue)
            {
                PutByte((sbyte)value);
            }
            else if (value >= short.MinValue && value <= short.MaxValue)
            {
                PutShort((short)value);
            }
            else if (value >= int.MinValue && value <= int.MaxValue)
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
        /// Appends a long.
        /// </summary>
        /// <param name="value">Value.</param>
        public void AppendLongNullable(long? value)
        {
            if (value == null)
            {
                AppendNull();
            }
            else
            {
                AppendLong(value.Value);
            }
        }

        /// <summary>
        /// Appends a gloat.
        /// </summary>
        /// <param name="value">Value.</param>
        public void AppendFloat(float value)
        {
            if (GetHashOrder() is { } hashOrder)
            {
                PutHash(hashOrder, HashUtils.Hash32(value));
            }

            PutFloat(value);
            OnWrite();
        }

        /// <summary>
        /// Appends a gloat.
        /// </summary>
        /// <param name="value">Value.</param>
        public void AppendFloatNullable(float? value)
        {
            if (value == null)
            {
                AppendNull();
            }
            else
            {
                AppendFloat(value.Value);
            }
        }

        /// <summary>
        /// Appends a double.
        /// </summary>
        /// <param name="value">Value.</param>
        public void AppendDouble(double value)
        {
            if (GetHashOrder() is { } hashOrder)
            {
                PutHash(hashOrder, HashUtils.Hash32(value));
            }

            // ReSharper disable once CompareOfFloatsByEqualityOperator
            if (value == (float)value)
            {
                PutFloat((float)value);
            }
            else
            {
                PutDouble(value);
            }

            OnWrite();
        }

        /// <summary>
        /// Appends a double.
        /// </summary>
        /// <param name="value">Value.</param>
        public void AppendDoubleNullable(double? value)
        {
            if (value == null)
            {
                AppendNull();
            }
            else
            {
                AppendDouble(value.Value);
            }
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
            }
            else
            {
                AppendString(value);
            }
        }

        /// <summary>
        /// Appends bytes.
        /// </summary>
        /// <param name="value">Value.</param>
        public void AppendBytes(Span<byte> value)
        {
            if (GetHashOrder() is { } hashOrder)
            {
                PutHash(hashOrder, HashUtils.Hash32(value));
            }

            PutBytes(value);
            OnWrite();
        }

        /// <summary>
        /// Appends bytes.
        /// </summary>
        /// <param name="value">Value.</param>
        public void AppendBytes(byte[] value) => AppendBytes(value.AsSpan());

        /// <summary>
        /// Appends bytes.
        /// </summary>
        /// <param name="value">Value.</param>
        public void AppendBytesNullable(byte[]? value)
        {
            if (value == null)
            {
                AppendNull();
            }
            else
            {
                AppendBytes(value);
            }
        }

        /// <summary>
        /// Appends a guid.
        /// </summary>
        /// <param name="value">Value.</param>
        public void AppendGuid(Guid value)
        {
            var span = GetSpan(16);
            UuidSerializer.Write(value, span);

            if (GetHashOrder() is { } hashOrder)
            {
                var lo = BinaryPrimitives.ReadInt64LittleEndian(span[..8]);
                var hi = BinaryPrimitives.ReadInt64LittleEndian(span[8..]);
                var hash = HashUtils.Hash32(hi, HashUtils.Hash32(lo));

                PutHash(hashOrder, hash);
            }

            OnWrite();
        }

        /// <summary>
        /// Appends a guid.
        /// </summary>
        /// <param name="value">Value.</param>
        public void AppendGuidNullable(Guid? value)
        {
            if (value == null)
            {
                AppendNull();
            }
            else
            {
                AppendGuid(value.Value);
            }
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

                // Trim zero bytes.
                while (size > 0 && arr[size - 1] == 0)
                {
                    size--;
                }

                var resBytes = arr.AsSpan()[..size];

                if (GetHashOrder() is { } hashOrder)
                {
                    PutHash(hashOrder, HashUtils.Hash32(resBytes));
                }

                PutBytes(resBytes);

                OnWrite();
            }
            finally
            {
                ByteArrayPool.Return(arr);
            }
        }

        /// <summary>
        /// Appends a bitmask.
        /// </summary>
        /// <param name="value">Value.</param>
        public void AppendBitmaskNullable(BitArray? value)
        {
            if (value == null)
            {
                AppendNull();
            }
            else
            {
                AppendBitmask(value);
            }
        }

        /// <summary>
        /// Appends a decimal.
        /// </summary>
        /// <param name="value">Value.</param>
        /// <param name="scale">Decimal scale from schema.</param>
        public void AppendDecimal(decimal value, int scale)
        {
            var (unscaled, actualScale) = BinaryTupleCommon.DecimalToUnscaledBigInteger(value, scale);

            PutShort(actualScale);
            AppendNumber(unscaled);
        }

        /// <summary>
        /// Appends a decimal.
        /// </summary>
        /// <param name="value">Value.</param>
        /// <param name="scale">Decimal scale from schema.</param>
        public void AppendDecimalNullable(decimal? value, int scale)
        {
            if (value == null)
            {
                AppendNull();
            }
            else
            {
                AppendDecimal(value.Value, scale);
            }
        }

        /// <summary>
        /// Appends a number.
        /// </summary>
        /// <param name="value">Value.</param>
        public void AppendNumber(BigInteger value)
        {
            var size = value.GetByteCount();
            var destination = GetSpan(size);
            var success = value.TryWriteBytes(destination, out int written, isBigEndian: true);

            if (GetHashOrder() is { } hashOrder)
            {
                PutHash(hashOrder, HashUtils.Hash32(destination[..written]));
            }

            Debug.Assert(success, "success");
            Debug.Assert(written == size, "written == size");

            OnWrite();
        }

        /// <summary>
        /// Appends a number.
        /// </summary>
        /// <param name="value">Value.</param>
        public void AppendNumberNullable(BigInteger? value)
        {
            if (value == null)
            {
                AppendNull();
            }
            else
            {
                AppendNumber(value.Value);
            }
        }

        /// <summary>
        /// Appends a date.
        /// </summary>
        /// <param name="value">Value.</param>
        public void AppendDate(LocalDate value)
        {
            if (GetHashOrder() is { } hashOrder)
            {
                PutHash(hashOrder, HashUtils.Hash32(value));
            }

            PutDate(value);
            OnWrite();
        }

        /// <summary>
        /// Appends a date.
        /// </summary>
        /// <param name="value">Value.</param>
        public void AppendDateNullable(LocalDate? value)
        {
            if (value == null)
            {
                AppendNull();
            }
            else
            {
                AppendDate(value.Value);
            }
        }

        /// <summary>
        /// Appends a time.
        /// </summary>
        /// <param name="value">Value.</param>
        /// <param name="precision">Precision.</param>
        public void AppendTime(LocalTime value, int precision)
        {
            if (GetHashOrder() is { } hashOrder)
            {
                PutHash(hashOrder, HashUtils.Hash32(value, precision));
            }

            PutTime(value, precision);
            OnWrite();
        }

        /// <summary>
        /// Appends a time.
        /// </summary>
        /// <param name="value">Value.</param>
        /// <param name="precision">Precision.</param>
        public void AppendTimeNullable(LocalTime? value, int precision)
        {
            if (value == null)
            {
                AppendNull();
            }
            else
            {
                AppendTime(value.Value, precision);
            }
        }

        /// <summary>
        /// Appends a date and time.
        /// </summary>
        /// <param name="value">Value.</param>
        /// <param name="precision">Precision.</param>
        public void AppendDateTime(LocalDateTime value, int precision)
        {
            if (GetHashOrder() is { } hashOrder)
            {
                PutHash(hashOrder, HashUtils.Hash32(value, precision));
            }

            PutDate(value.Date);
            PutTime(value.TimeOfDay, precision);
            OnWrite();
        }

        /// <summary>
        /// Appends a date and time.
        /// </summary>
        /// <param name="value">Value.</param>
        /// <param name="precision">Precision.</param>
        public void AppendDateTimeNullable(LocalDateTime? value, int precision)
        {
            if (value == null)
            {
                AppendNull();
            }
            else
            {
                AppendDateTime(value.Value, precision);
            }
        }

        /// <summary>
        /// Appends a timestamp (instant).
        /// </summary>
        /// <param name="value">Value.</param>
        /// <param name="precision">Precision.</param>
        public void AppendTimestamp(Instant value, int precision)
        {
            var (seconds, nanos) = PutTimestamp(value, precision);

            if (GetHashOrder() is { } hashOrder)
            {
                var hash = HashUtils.Hash32(nanos, HashUtils.Hash32(seconds));
                PutHash(hashOrder, hash);
            }

            OnWrite();
        }

        /// <summary>
        /// Appends a timestamp (instant).
        /// </summary>
        /// <param name="value">Value.</param>
        /// <param name="precision">Precision.</param>
        public void AppendTimestampNullable(Instant? value, int precision)
        {
            if (value == null)
            {
                AppendNull();
            }
            else
            {
                AppendTimestamp(value.Value, precision);
            }
        }

        /// <summary>
        /// Appends a duration.
        /// </summary>
        /// <param name="value">Value.</param>
        public void AppendDuration(Duration value)
        {
            if (GetHashOrder() is not null)
            {
                // Colocation keys can't include Duration.
                throw new NotSupportedException("Duration hashing is not supported.");
            }

            PutDuration(value);
            OnWrite();
        }

        /// <summary>
        /// Appends a duration.
        /// </summary>
        /// <param name="value">Value.</param>
        public void AppendDurationNullable(Duration? value)
        {
            if (value == null)
            {
                AppendNull();
            }
            else
            {
                AppendDuration(value.Value);
            }
        }

        /// <summary>
        /// Appends a period.
        /// </summary>
        /// <param name="value">Value.</param>
        public void AppendPeriod(Period value)
        {
            if (GetHashOrder() is not null)
            {
                // Colocation keys can't include Period.
                throw new NotSupportedException("Period hashing is not supported.");
            }

            PutPeriod(value);
            OnWrite();
        }

        /// <summary>
        /// Appends a period.
        /// </summary>
        /// <param name="value">Value.</param>
        public void AppendPeriodNullable(Period? value)
        {
            if (value == null)
            {
                AppendNull();
            }
            else
            {
                AppendPeriod(value);
            }
        }

        /// <summary>
        /// Appends an object.
        /// </summary>
        /// <param name="value">Value.</param>
        /// <param name="colType">Column type.</param>
        /// <param name="scale">Decimal scale.</param>
        /// <param name="precision">Precision.</param>
        public void AppendObject(object? value, ColumnType colType, int scale = 0, int precision = TemporalTypes.MaxTimePrecision)
        {
            if (value == null)
            {
                AppendNull();
                return;
            }

            switch (colType)
            {
                case ColumnType.Int8:
                    AppendByte((sbyte)value);
                    break;

                case ColumnType.Int16:
                    AppendShort((short)value);
                    break;

                case ColumnType.Int32:
                    AppendInt((int)value);
                    break;

                case ColumnType.Int64:
                    AppendLong((long)value);
                    break;

                case ColumnType.Float:
                    AppendFloat((float)value);
                    break;

                case ColumnType.Double:
                    AppendDouble((double)value);
                    break;

                case ColumnType.Uuid:
                    AppendGuid((Guid)value);
                    break;

                case ColumnType.String:
                    AppendString((string)value);
                    break;

                case ColumnType.ByteArray:
                    AppendBytes((byte[])value);
                    break;

                case ColumnType.Bitmask:
                    AppendBitmask((BitArray)value);
                    break;

                case ColumnType.Decimal:
                    AppendDecimal((decimal)value, scale);
                    break;

                case ColumnType.Number:
                    AppendNumber((BigInteger)value);
                    break;

                case ColumnType.Date:
                    AppendDate((LocalDate)value);
                    break;

                case ColumnType.Time:
                    AppendTime((LocalTime)value, precision);
                    break;

                case ColumnType.Datetime:
                    AppendDateTime((LocalDateTime)value, precision);
                    break;

                case ColumnType.Timestamp:
                    AppendTimestamp((Instant)value, precision);
                    break;

                case ColumnType.Boolean:
                    AppendBool((bool)value);
                    break;

                default:
                    throw new IgniteClientException(ErrorGroups.Client.Protocol, "Unsupported type: " + colType);
            }
        }

        /// <summary>
        /// Appends an object.
        /// </summary>
        /// <param name="value">Value.</param>
        /// <param name="timePrecision">Time precision.</param>
        /// <param name="timestampPrecision">Timestamp precision.</param>
        public void AppendObjectWithType(
            object? value,
            int timePrecision = TemporalTypes.MaxTimePrecision,
            int timestampPrecision = TemporalTypes.MaxTimePrecision)
        {
            switch (value)
            {
                case null:
                    AppendNull(); // Type.
                    AppendNull(); // Scale.
                    AppendNull(); // Value.
                    break;

                case int i32:
                    AppendTypeAndScale(ColumnType.Int32);
                    AppendInt(i32);
                    break;

                case long i64:
                    AppendTypeAndScale(ColumnType.Int64);
                    AppendLong(i64);
                    break;

                case string str:
                    AppendTypeAndScale(ColumnType.String);
                    AppendString(str);
                    break;

                case Guid uuid:
                    AppendTypeAndScale(ColumnType.Uuid);
                    AppendGuid(uuid);
                    break;

                case sbyte i8:
                    AppendTypeAndScale(ColumnType.Int8);
                    AppendByte(i8);
                    break;

                case short i16:
                    AppendTypeAndScale(ColumnType.Int16);
                    AppendShort(i16);
                    break;

                case float f32:
                    AppendTypeAndScale(ColumnType.Float);
                    AppendFloat(f32);
                    break;

                case double f64:
                    AppendTypeAndScale(ColumnType.Double);
                    AppendDouble(f64);
                    break;

                case byte[] bytes:
                    AppendTypeAndScale(ColumnType.ByteArray);
                    AppendBytes(bytes);
                    break;

                case decimal dec:
                    var scale = GetDecimalScale(dec);
                    AppendTypeAndScale(ColumnType.Decimal, scale);
                    AppendDecimal(dec, scale);
                    break;

                case BigInteger bigInt:
                    AppendTypeAndScale(ColumnType.Number);
                    AppendNumber(bigInt);
                    break;

                case LocalDate localDate:
                    AppendTypeAndScale(ColumnType.Date);
                    AppendDate(localDate);
                    break;

                case LocalTime localTime:
                    AppendTypeAndScale(ColumnType.Time);
                    AppendTime(localTime, timePrecision);
                    break;

                case LocalDateTime localDateTime:
                    AppendTypeAndScale(ColumnType.Datetime);
                    AppendDateTime(localDateTime, timePrecision);
                    break;

                case Instant instant:
                    AppendTypeAndScale(ColumnType.Timestamp);
                    AppendTimestamp(instant, timestampPrecision);
                    break;

                case BitArray bitArray:
                    AppendTypeAndScale(ColumnType.Bitmask);
                    AppendBitmask(bitArray);
                    break;

                default:
                    throw new IgniteClientException(ErrorGroups.Client.Protocol, "Unsupported type: " + value.GetType());
            }
        }

        /// <summary>
        /// Appends an object.
        /// </summary>
        /// <param name="collection">Value.</param>
        /// <typeparam name="T">Element type.</typeparam>
        public void AppendObjectCollectionWithType<T>(Span<T> collection)
        {
            var firstValue = collection[0];

            switch (firstValue)
            {
                case int:
                    AppendTypeAndSize(ColumnType.Int32, collection.Length);
                    foreach (var item in collection)
                    {
                        AppendInt((int)(object)item!);
                    }

                    break;

                case long:
                    AppendTypeAndSize(ColumnType.Int64, collection.Length);
                    foreach (var item in collection)
                    {
                        AppendLong((long)(object)item!);
                    }

                    break;

                case string:
                    AppendTypeAndSize(ColumnType.String, collection.Length);
                    foreach (var item in collection)
                    {
                        AppendString((string)(object)item!);
                    }

                    break;

                case Guid:
                    AppendTypeAndSize(ColumnType.Uuid, collection.Length);
                    foreach (var item in collection)
                    {
                        AppendGuid((Guid)(object)item!);
                    }

                    break;

                case sbyte:
                    AppendTypeAndSize(ColumnType.Int8, collection.Length);
                    foreach (var item in collection)
                    {
                        AppendByte((sbyte)(object)item!);
                    }

                    break;

                case short:
                    AppendTypeAndSize(ColumnType.Int16, collection.Length);
                    foreach (var item in collection)
                    {
                        AppendShort((short)(object)item!);
                    }

                    break;

                case float:
                    AppendTypeAndSize(ColumnType.Float, collection.Length);
                    foreach (var item in collection)
                    {
                        AppendFloat((float)(object)item!);
                    }

                    break;

                case double:
                    AppendTypeAndSize(ColumnType.Double, collection.Length);
                    foreach (var item in collection)
                    {
                        AppendDouble((double)(object)item!);
                    }

                    break;

                case (byte[]):
                    AppendTypeAndSize(ColumnType.ByteArray, collection.Length);
                    foreach (var item in collection)
                    {
                        AppendBytes((byte[])(object)item!);
                    }

                    break;

                case decimal:
                    AppendTypeAndSize(ColumnType.Decimal, collection.Length);
                    foreach (var item in collection)
                    {
                        AppendDecimal((decimal)(object)item!, int.MaxValue);
                    }

                    break;

                case BigInteger:
                    AppendTypeAndSize(ColumnType.Number, collection.Length);
                    foreach (var item in collection)
                    {
                        AppendNumber((BigInteger)(object)item!);
                    }

                    break;

                case LocalDate:
                    AppendTypeAndSize(ColumnType.Date, collection.Length);
                    foreach (var item in collection)
                    {
                        AppendDate((LocalDate)(object)item!);
                    }

                    break;

                case LocalTime:
                    AppendTypeAndSize(ColumnType.Time, collection.Length);
                    foreach (var item in collection)
                    {
                        AppendTime((LocalTime)(object)item!, TemporalTypes.MaxTimePrecision);
                    }

                    break;

                case LocalDateTime:
                    AppendTypeAndSize(ColumnType.Datetime, collection.Length);
                    foreach (var item in collection)
                    {
                        AppendDateTime((LocalDateTime)(object)item!, TemporalTypes.MaxTimePrecision);
                    }

                    break;

                case Instant:
                    AppendTypeAndSize(ColumnType.Timestamp, collection.Length);
                    foreach (var item in collection)
                    {
                        AppendTimestamp((Instant)(object)item!, TemporalTypes.MaxTimePrecision);
                    }

                    break;

                case BitArray:
                    AppendTypeAndSize(ColumnType.Bitmask, collection.Length);
                    foreach (var item in collection)
                    {
                        AppendBitmask((BitArray)(object)item!);
                    }

                    break;

                default:
                    throw new IgniteClientException(ErrorGroups.Client.Protocol, "Unsupported type: " + firstValue?.GetType());
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
            int baseOffset = _entryBase - BinaryTupleCommon.HeaderSize;
            int offset = baseOffset;

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

                offset = baseOffset + (_entrySize - desiredEntrySize) * _numElements;
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

        private static int GetDecimalScale(decimal value)
        {
            Span<int> bits = stackalloc int[4];
            decimal.GetBits(value, bits);

            return (bits[3] & 0x00FF0000) >> 16;
        }

        private void PutByte(sbyte value) => _buffer.WriteByte(unchecked((byte)value));

        private void PutShort(short value) => _buffer.WriteShort(value);

        private void PutInt(int value) => _buffer.WriteInt(value);

        private void PutLong(long value) => _buffer.WriteLong(value);

        private void PutFloat(float value) => PutInt(BitConverter.SingleToInt32Bits(value));

        private void PutDouble(double value) => PutLong(BitConverter.DoubleToInt64Bits(value));

        private void PutBytes(Span<byte> bytes)
        {
            if (bytes.Length == 0)
            {
                GetSpan(1)[0] = BinaryTupleCommon.VarlenEmptyByte;
            }
            else if (bytes[0] == BinaryTupleCommon.VarlenEmptyByte)
            {
                var span = GetSpan(bytes.Length + 1);
                span[0] = BinaryTupleCommon.VarlenEmptyByte;
                bytes.CopyTo(span[1..]);
            }
            else
            {
                bytes.CopyTo(GetSpan(bytes.Length));
            }
        }

        private void PutString(string value)
        {
            if (value.Length == 0)
            {
                if (GetHashOrder() is { } hashOrder)
                {
                    PutHash(hashOrder, HashUtils.Hash32(Span<byte>.Empty));
                }

                _buffer.GetSpan(1)[0] = BinaryTupleCommon.VarlenEmptyByte;
                _buffer.Advance(1);
                return;
            }

            var maxByteCount = ProtoCommon.StringEncoding.GetMaxByteCount(value.Length);
            var span = _buffer.GetSpan(maxByteCount);

            var actualBytes = ProtoCommon.StringEncoding.GetBytes(value, span);
            span = span[..actualBytes];

            if (GetHashOrder() is { } hashOrder2)
            {
                PutHash(hashOrder2, HashUtils.Hash32(span));
            }

            _buffer.Advance(actualBytes);

            // UTF-8 encoded strings should not start with 0x80 (character codes larger than 127 have a multi-byte encoding).
            // We trust this but verify.
            if (span[0] == BinaryTupleCommon.VarlenEmptyByte)
            {
                throw new InvalidOperationException(
                    $"Failed to encode a string element: resulting payload starts with invalid {BinaryTupleCommon.VarlenEmptyByte} byte");
            }
        }

        private (long Seconds, int Nanos) PutTimestamp(Instant value, int precision)
        {
            var (seconds, nanos) = value.ToSecondsAndNanos(precision);

            PutLong(seconds);

            if (nanos != 0)
            {
                PutInt(nanos);
            }

            return (seconds, nanos);
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

        private void PutTime(LocalTime value, int precision)
        {
            long hour = value.Hour;
            long minute = value.Minute;
            long second = value.Second;
            long nanos = TemporalTypes.NormalizeNanos(value.NanosecondOfSecond, precision);

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

        private void AppendTypeAndScale(ColumnType type, int scale = 0)
        {
            AppendInt((int)type);
            AppendInt(scale);
        }

        private void AppendTypeAndSize(ColumnType type, int size)
        {
            AppendInt((int)type);
            AppendInt(size);
        }

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

        private int? GetHashOrder() => _hashedColumnsPredicate?.HashedColumnOrder(_elementIndex) switch
        {
            null or < 0 => null,
            { } order => order
        };

        private void PutHash(int index, int hash)
        {
            Debug.Assert(_hashedColumnsPredicate != null, "_hashedColumnsPredicate != null");
            Debug.Assert(index >= 0, "index >= 0");
            Debug.Assert(index < _hashedColumnsPredicate.HashedColumnCount, "index < _hashedColumnsPredicate.HashedColumnCount");

            GetHashSpan()[index] = hash;
        }

        private Span<int> GetHashSpan() => MemoryMarshal.Cast<byte, int>(_buffer.GetWrittenMemory().Span);
    }
}
