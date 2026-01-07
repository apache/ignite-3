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

namespace Apache.Ignite.Internal;

using System.Threading;

/// <summary>
/// Hybrid timestamp tracker.
/// </summary>
internal sealed class HybridTimestampTracker
{
    private long _val;

    /// <summary>
    /// Gets the current value.
    /// </summary>
    public long Value => Interlocked.Read(ref _val);

    /// <summary>
    /// Updates the timestamp to max(newVal, currentVal).
    /// </summary>
    /// <param name="newVal">New value.</param>
    /// <returns>Previous value.</returns>
    public long GetAndUpdate(long newVal)
    {
        // Atomically update the observable timestamp to max(newTs, curTs).
        long current = Interlocked.Read(ref _val);

        while (true)
        {
            if (current >= newVal)
            {
                // Already up to date or ahead.
                return current;
            }

            long previous = Interlocked.CompareExchange(ref _val, newVal, current);

            if (previous == current)
            {
                // Update succeeded.
                return current;
            }

            // Update failed, another thread changed the value.
            current = previous;
        }
    }
}
