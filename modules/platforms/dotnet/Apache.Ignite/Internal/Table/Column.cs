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

namespace Apache.Ignite.Internal.Table;

using Ignite.Sql;

/// <summary>
/// Schema column.
/// </summary>
internal record Column(
    string Name,
    ColumnType Type,
    bool IsNullable,
    int KeyIndex,
    int ColocationIndex,
    int SchemaIndex,
    int Scale,
    int Precision)
{
    /// <summary>
    /// Gets a value indicating whether this column is a part of the key.
    /// </summary>
    public bool IsKey => KeyIndex >= 0;

    /// <summary>
    /// Gets a value indicating whether this column is a part of the colocation key.
    /// </summary>
    public bool IsColocation => ColocationIndex >= 0;

    /// <summary>
    /// Gets the column index within a binary tuple.
    /// </summary>
    /// <param name="keyOnly">Whether a key-only binary tuple is used.</param>
    /// <returns>Index within a binary tuple.</returns>
    public int GetBinaryTupleIndex(bool keyOnly) => keyOnly ? KeyIndex : SchemaIndex;
}
