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

namespace Apache.Ignite.Internal.Table.Serialization.Mappers;

/// <summary>
/// Key-value mapper helpers.
/// </summary>
internal static class KeyValueMappers
{
    /// <summary>
    /// Creates a key-value pair mapper for the specified types if supported; otherwise, returns null.
    /// </summary>
    /// <typeparam name="TKey">Key type.</typeparam>
    /// <typeparam name="TValue">Value type.</typeparam>
    /// <returns>Key-value pair mapper or null.</returns>
    public static KeyValuePairCompositeMapper<TKey, TValue>? TryCreate<TKey, TValue>()
    {
        var keyMapper = OneColumnMappers.TryCreate<TKey>();
        var valueMapper = OneColumnMappers.TryCreate<TValue>();

        if (keyMapper == null || valueMapper == null)
        {
            return null;
        }

        return new KeyValuePairCompositeMapper<TKey, TValue>(keyMapper, valueMapper);
    }
}
