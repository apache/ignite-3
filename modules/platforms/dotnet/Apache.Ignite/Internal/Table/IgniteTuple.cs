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
    using Ignite.Table;

    /// <summary>
    /// Ignite tuple.
    /// </summary>
    internal class IgniteTuple : IIgniteTuple
    {
        /** Key-value pairs. */
        private readonly List<KeyValuePair<string, object?>> _pairs;

        /** Column index map. */
        private readonly Dictionary<string, int> _indexes;

        /// <summary>
        /// Initializes a new instance of the <see cref="IgniteTuple"/> class.
        /// </summary>
        /// <param name="capacity">Capacity.</param>
        public IgniteTuple(int capacity = 16)
        {
            _pairs = new List<KeyValuePair<string, object?>>(capacity);
            _indexes = new Dictionary<string, int>(capacity);
        }

        /// <inheritdoc/>
        public int FieldCount => _pairs.Count;

        /// <inheritdoc/>
        public object? this[int ordinal] => _pairs[ordinal].Value;

        /// <inheritdoc/>
        public object? this[string name]
        {
            get => _pairs[_indexes[name]].Value;
            set
            {
                var pair = new KeyValuePair<string, object?>(name, value);

                if (_indexes.TryGetValue(name, out var index))
                {
                    _pairs[index] = pair;
                }
                else
                {
                    index = FieldCount;
                    _indexes[name] = index;
                    _pairs.Add(pair);
                }
            }
        }

        /// <inheritdoc/>
        public string GetName(int ordinal) => _pairs[ordinal].Key;

        /// <inheritdoc/>
        public int GetOrdinal(string name) => _indexes[name];
    }
}
