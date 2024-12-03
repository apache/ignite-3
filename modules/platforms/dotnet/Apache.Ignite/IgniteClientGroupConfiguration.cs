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

/// <summary>
/// Ignite client group configuration. See <see cref="IgniteClientGroup"/> for more details.
/// </summary>
public sealed record IgniteClientGroupConfiguration
{
    /// <summary>
    /// Gets or sets the group size (maximum number of client instances - see <see cref="IIgniteClient"/>).
    /// <para />
    /// Defaults to 1.
    /// One client is enough for most use cases: it is thread-safe, implements request multiplexing, and connects to multiple cluster nodes.
    /// </summary>
    public int Size { get; set; } = 1;

    /// <summary>
    /// Gets or sets the client configuration.
    /// </summary>
    public required IgniteClientConfiguration ClientConfiguration { get; set; }
}
