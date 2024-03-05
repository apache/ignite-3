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

using System.Collections.Generic;
using Proto.BinaryTuple;

/// <summary>
/// Schema-based hashed column index provider.
/// </summary>
internal sealed class HashedColumnIndexProvider : IHashedColumnIndexProvider
{
    private readonly IReadOnlyList<Column> _columns;

    /// <summary>
    /// Initializes a new instance of the <see cref="HashedColumnIndexProvider"/> class.
    /// </summary>
    /// <param name="columns">Columns.</param>
    /// <param name="hashedColumnCount">Hashed column count.</param>
    public HashedColumnIndexProvider(IReadOnlyList<Column> columns, int hashedColumnCount)
    {
        _columns = columns;
        HashedColumnCount = hashedColumnCount;
    }

    /// <inheritdoc/>
    public int HashedColumnCount { get; init; }

    /// <inheritdoc/>
    public int HashedColumnOrder(int index) => _columns[index].ColocationIndex;
}
