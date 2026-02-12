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

using System.Collections.Concurrent;
using System.Collections.Generic;

/// <summary>
/// Concurrent cache with eviction based on capacity.
/// </summary>
/// <typeparam name="TKey">Key type.</typeparam>
/// <typeparam name="TValue">Value type.</typeparam>
internal sealed class ConcurrentCache<TKey, TValue>
    where TKey : notnull
{
    private readonly int _capacity;
    private readonly ConcurrentDictionary<TKey, Entry> _map;
    private readonly IEnumerator<KeyValuePair<TKey, Entry>> _hand;

    /// <summary>
    /// Initializes a new instance of the <see cref="ConcurrentCache{TKey, TValue}"/> class.
    /// </summary>
    /// <param name="capacity">Maximum capacity.</param>
    public ConcurrentCache(int capacity)
    {
        _capacity = capacity;
        _map = new ConcurrentDictionary<TKey, Entry>();
        _hand = _map.GetEnumerator();
    }

    /// <summary>
    /// Adds the specified key and value to the cache. Evicts old entries if capacity is exceeded.
    /// </summary>
    /// <param name="key">Key.</param>
    /// <param name="value">Value.</param>
    /// <returns>True if the entry was added, false if an entry with the same key already exists.</returns>
    public bool TryAdd(TKey key, TValue value)
    {
        var added = _map.TryAdd(key, new Entry(value));

        if (added)
        {
            EvictIfNeeded();
        }

        return added;
    }

    /// <summary>
    /// Gets the value associated with the specified key.
    /// </summary>
    /// <param name="key">Key.</param>
    /// <returns>Value, or default(TValue) if not exists.</returns>
    public TValue? GetValueOrDefault(TKey key)
    {
        if (!_map.TryGetValue(key, out var entry))
        {
            return default;
        }

        entry.Visited = true;
        return entry.Value;
    }

    private void EvictIfNeeded()
    {
        if (_map.Count <= _capacity)
        {
            return;
        }

        lock (_hand)
        {
            while (_map.Count > _capacity)
            {
                // SIEVE-like eviction.
                if (!_hand.MoveNext())
                {
                    _hand.Reset();
                    continue;
                }

                var current = _hand.Current;

                if (current.Value.Visited)
                {
                    current.Value.Visited = false;
                    continue;
                }

                _map.TryRemove(current.Key, out _);
            }
        }
    }

    private sealed record Entry(TValue Value)
    {
        public bool Visited { get; set; }
    }
}
