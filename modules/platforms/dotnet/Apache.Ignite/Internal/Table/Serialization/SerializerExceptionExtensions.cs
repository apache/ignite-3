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

namespace Apache.Ignite.Internal.Table.Serialization;

using System;
using System.Collections.Generic;
using Common;

/// <summary>
/// Serializer-related exception extensions.
/// </summary>
internal static class SerializerExceptionExtensions
{
    private const string UnmappedColumnsPresent = nameof(UnmappedColumnsPresent);

    /// <summary>
    /// Gets a value indicating whether the specified exception was caused by unmapped columns.
    /// </summary>
    /// <param name="e">Exception.</param>
    /// <returns>True when the the specified exception was caused by unmapped columns; false otherwise.</returns>
    public static bool CausedByUnmappedColumns(this Exception e) => e.Data[UnmappedColumnsPresent] is true;

    /// <summary>
    /// Gets an exception indicating schema mismatch due to unmapped (extra) columns.
    /// </summary>
    /// <param name="prefix">Message prefix (indicates what we are trying to serialize).</param>
    /// <param name="schema">Schema.</param>
    /// <param name="unmappedColumns">Unmapped columns.</param>
    /// <returns>Exception.</returns>
    public static ArgumentException GetUnmappedColumnsException(string prefix, Schema schema, IEnumerable<string> unmappedColumns) =>
        new($"{prefix} doesn't match schema: schemaVersion={schema.Version}, extraColumns={unmappedColumns.StringJoin()}")
        {
            Data = { [UnmappedColumnsPresent] = true }
        };
}
