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

namespace Apache.Ignite.Table
{
    using Internal.Table;

    /// <summary>
    /// Ignite Tuple.
    /// <para />
    /// Use <see cref="Create()"/> to create an instance.
    /// </summary>
    public interface IIgniteTuple
    {
        /// <summary>
        /// Gets the number of columns.
        /// </summary>
        int FieldCount { get; }

        /// <summary>
        /// Gets the value of the specified column as an object.
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        object? this[int ordinal] { get; }

        /// <summary>
        /// Gets the value of the specified column as an object.
        /// </summary>
        /// <param name="name">The column name.</param>
        object? this[string name] { get; set; }

        /// <summary>
        /// Gets the name of the column, given the zero-based column ordinal.
        /// </summary>
        /// <param name="ordinal">Zero-based column ordinal.</param>
        /// <returns>Column name.</returns>
        string GetName(int ordinal);

        /// <summary>
        /// Gets the column ordinal given the name of the column.
        /// </summary>
        /// <param name="name">Column name.</param>
        /// <returns>Column ordinal.</returns>
        int GetOrdinal(string name);

        /// <summary>
        /// Creates a new tuple.
        /// </summary>
        /// <returns>New tuple.</returns>
        static IIgniteTuple Create() => new IgniteTuple();

        /// <summary>
        /// Creates a new tuple.
        /// </summary>
        /// <param name="capacity">Capacity.</param>
        /// <returns>New tuple.</returns>
        static IIgniteTuple Create(int capacity) => new IgniteTuple(capacity);
    }
}
