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
    internal sealed class BinaryTupleReader
    {
        /** Buffer. */
        private readonly Memory<byte> _buffer;

        /** Number of elements in the tuple. */
        private readonly int _numElements;

        /** Size of an offset table entry. */
        private readonly int _entrySize;

        /** Position of the varlen offset table. */
        private readonly int _entryBase;

        /** Starting position of variable-length values. */
        private readonly int _valueBase;

        /// <summary>
        /// Initializes a new instance of the <see cref="BinaryTupleReader"/> class.
        /// </summary>
        /// <param name="buffer">Buffer.</param>
        /// <param name="numElements">Number of elements in the tuple.</param>
        public BinaryTupleReader(Memory<byte> buffer, int numElements)
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

        private int GetOffset(int position)
        {
            var span = _buffer.Span[position..];

            switch (_entrySize)
            {
                case 1:
                    return span[position];

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

        private void Seek(int index)
        {
            Debug.Assert(index >= 0, "index >= 0");
            Debug.Assert(index < _numElements, "index < numElements");

            int entry = _entryBase + index * _entrySize;

            int offset = _valueBase;
            if (index > 0)
            {
                offset += GetOffset(entry - _entrySize);
            }

            int nextOffset = valueBase + getOffset(entry);
            if (nextOffset < offset) {
                throw new BinaryTupleFormatException("Corrupted offset table");
            }

            if (offset == nextOffset && hasNullMap()) {
                int nullIndex = BinaryTupleCommon.nullOffset(index);
                byte nullMask = BinaryTupleCommon.nullMask(index);
                if ((buffer.get(nullIndex) & nullMask) != 0) {
                    sink.nextElement(index, 0, 0);
                    return;
                }
            }

            sink.nextElement(index, offset, nextOffset);

        }
    }
}
