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
    using System.Diagnostics;

    /// <summary>
    /// Binary tuple reader.
    /// </summary>
    internal readonly ref struct BinaryTupleReader // TODO: Support all types (IGNITE-15431).
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
        /// Gets an object value according to the specified type.
        /// </summary>
        /// <param name="index">Index.</param>
        /// <param name="columnType">Column type.</param>
        /// <returns>Value.</returns>
        public object? GetObject(int index, ClientDataType columnType)
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

                // TODO: Support all types (IGNITE-15431).
                _ => throw new IgniteClientException("Unsupported type: " + columnType)
            };
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
