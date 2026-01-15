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

namespace Apache.Extensions.Caching.Ignite.Internal;

using System.Diagnostics.CodeAnalysis;

/// <summary>
/// Cache entry DTO.
/// </summary>
/// <param name="Value">Value bytes.</param>
/// <param name="ExpiresAt">Expiration (Unix time ticks).</param>
/// <param name="SlidingExpiration">Sliding expiration (ticks).</param>
[SuppressMessage("Performance", "CA1819:Properties should not return arrays", Justification = "Internal DTO")]
internal record struct CacheEntry(
    byte[] Value,
    long? ExpiresAt,
    long? SlidingExpiration);
