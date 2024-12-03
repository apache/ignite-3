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

namespace Apache.Ignite;

using System.Collections.Generic;

/// <summary>
/// Collection extensions.
/// </summary>
public static class CollectionExtensions
{
#pragma warning disable CS1998 // Async method lacks 'await' operators and will run synchronously
    /// <summary>
    /// Converts enumerable to async enumerable.
    /// </summary>
    /// <param name="enumerable">Enumerable.</param>
    /// <typeparam name="T">Element type.</typeparam>
    /// <returns>Async enumerable.</returns>
    public static async IAsyncEnumerable<T> ToAsyncEnumerable<T>(this IEnumerable<T> enumerable)
    {
        foreach (var e in enumerable)
        {
            yield return e;
        }
    }
#pragma warning restore CS1998 // Async method lacks 'await' operators and will run synchronously
}
