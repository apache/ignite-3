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
    }
}
