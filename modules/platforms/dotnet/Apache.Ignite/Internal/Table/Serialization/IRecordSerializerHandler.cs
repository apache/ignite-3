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

namespace Apache.Ignite.Internal.Table.Serialization
{
    using System;
    using Proto.BinaryTuple;
    using Proto.MsgPack;

    /// <summary>
    /// Serializer handler.
    /// </summary>
    /// <typeparam name="T">Record type.</typeparam>
    internal interface IRecordSerializerHandler<T>
    {
        /// <summary>
        /// Reads a record.
        /// </summary>
        /// <param name="reader">Reader.</param>
        /// <param name="schema">Schema.</param>
        /// <param name="keyOnly">Key only mode.</param>
        /// <returns>Record.</returns>
        T Read(ref MsgPackReader reader, Schema schema, bool keyOnly = false);

        /// <summary>
        /// Reads the value part and combines with the specified key part into a new object.
        /// </summary>
        /// <param name="reader">Reader.</param>
        /// <param name="schema">Schema.</param>
        /// <param name="key">Key part.</param>
        /// <returns>Resulting record with key and value parts.</returns>
        T ReadValuePart(ref MsgPackReader reader, Schema schema, T key);

        /// <summary>
        /// Writes a record.
        /// </summary>
        /// <param name="writer">Writer.</param>
        /// <param name="schema">Schema.</param>
        /// <param name="record">Record.</param>
        /// <param name="keyOnly">Key only mode.</param>
        /// <param name="computeHash">Whether to compute key hash while writing the tuple.</param>
        /// <returns>Key hash when <paramref name="computeHash"/> is <c>true</c>; 0 otherwise.</returns>
        int Write(ref MsgPackWriter writer, Schema schema, T record, bool keyOnly = false, bool computeHash = false)
        {
            var columns = schema.Columns;
            var count = keyOnly ? schema.KeyColumnCount : columns.Count;
            var noValueSet = writer.WriteBitSet(count);

            var tupleBuilder = new BinaryTupleBuilder(count, hashedColumnsPredicate: computeHash ? schema : null);

            try
            {
                Write(ref tupleBuilder, record, schema, count, noValueSet);

                var binaryTupleMemory = tupleBuilder.Build();
                writer.Write(binaryTupleMemory.Span);

                return tupleBuilder.Hash;
            }
            finally
            {
                tupleBuilder.Dispose();
            }
        }

        /// <summary>
        /// Writes a record.
        /// </summary>
        /// <param name="tupleBuilder">Tuple builder.</param>
        /// <param name="record">Record.</param>
        /// <param name="schema">Schema.</param>
        /// <param name="columnCount">Column count.</param>
        /// <param name="noValueSet">No-value set.</param>
        void Write(
            ref BinaryTupleBuilder tupleBuilder,
            T record,
            Schema schema,
            int columnCount,
            Span<byte> noValueSet);
    }
}
