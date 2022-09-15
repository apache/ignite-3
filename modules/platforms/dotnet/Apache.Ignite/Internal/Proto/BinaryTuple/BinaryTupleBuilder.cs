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
    using System.Diagnostics;
    using Buffers;

    /// <summary>
    /// Binary tuple builder.
    /// </summary>
    internal ref struct BinaryTupleBuilder // TODO: Support all types (IGNITE-15431).
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
        /// Appends a guid.
        /// </summary>
        /// <param name="value">Value.</param>
        public void AppendGuid(Guid value)
        {
            if (value != default)
            {
                UuidSerializer.Write(value, GetSpan(16));
            }

            OnWrite();
        }

        /// <summary>
        /// Appends an object.
        /// </summary>
        /// <param name="value">Value.</param>
        /// <param name="colType">Column type.</param>
        public void AppendObject(object? value, ClientDataType colType)
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
                case ClientDataType.Decimal:
                default:
                    // TODO: Support all types (IGNITE-15431).
                    throw new IgniteClientException("Unsupported type: " + colType);
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
