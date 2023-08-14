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

namespace Apache.Ignite.Internal.Common;

using System.Collections.Generic;

/// <summary>
/// Collection extensions.
/// </summary>
internal static class CollectionExtensions
{
    /// <summary>
    /// Shortcut extension method for string.Join.
    /// </summary>
    /// <param name="source">Source collection.</param>
    /// <param name="separator">Separator.</param>
    /// <typeparam name="T">Element type.</typeparam>
    /// <returns>Resulting string.</returns>
    public static string StringJoin<T>(this IEnumerable<T> source, string separator = ", ") =>
        string.Join(separator, source);
}
