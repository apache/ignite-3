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

namespace Apache.Ignite.Internal.Sql;

using System.Diagnostics.CodeAnalysis;

/// <summary>
/// Partition awareness metadata returned by the server for SQL queries.
/// <para/>
/// The <see cref="Indexes"/> array describes how each element of a colocation key should be interpreted during evaluation:
/// <list type="bullet">
/// <item>If <c>indexes[i] >= 0</c>, then the value at position <c>i</c> in the colocation key should be taken
/// from a dynamic parameter at index <c>indexes[i]</c>.</item>
/// <item>If <c>indexes[i] &lt; 0</c>, then the value at position <c>i</c> is a constant literal whose precomputed
/// hash is stored in the <see cref="Hash"/> array at index <c>-(indexes[i] + 1)</c>.</item>
/// </list>
/// </summary>
[SuppressMessage("Performance", "CA1819:Properties should not return arrays", Justification = "Internal DTO.")]
internal sealed record SqlPartitionAwarenessMetadata(int TableId, int[] Indexes, int[] Hash);
