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

namespace Apache.Ignite.Internal.Table
{
    using System.Collections.Generic;
    using Proto.BinaryTuple;

    /// <summary>
    /// Schema.
    /// </summary>
    /// <param name="Version">Version.</param>
    /// <param name="KeyColumnCount">Key column count.</param>
    /// <param name="Columns">Columns in schema order.</param>
    internal sealed record Schema(
        int Version,
        int KeyColumnCount,
        IReadOnlyList<Column> Columns) : IColocationColumnIndexProvider
    {
        /// <summary>
        /// Gets the value column count.
        /// </summary>
        public int ValueColumnCount => Columns.Count - KeyColumnCount;

        /// <inheritdoc/>
        public bool IsColocationColumnIndex(int index) => index < KeyColumnCount && Columns[index].IsColocation;
    }
}
