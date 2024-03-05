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
    private readonly Schema _schema;
    private readonly IReadOnlyList<Column> _columns;

    /// <summary>
    /// Initializes a new instance of the <see cref="HashedColumnIndexProvider"/> class.
    /// </summary>
    /// <param name="schema">Schema.</param>
    /// <param name="keyOnly">Key only.</param>
    public HashedColumnIndexProvider(Schema schema, bool keyOnly)
    {
        _schema = schema;
        _columns = schema.GetColumnsFor(keyOnly);
    }

    /// <inheritdoc/>
    public int HashedColumnCount => _schema.ColocationColumnCount;

    /// <inheritdoc/>
    public int HashedColumnOrder(int index) => _columns[index].ColocationIndex;
}
