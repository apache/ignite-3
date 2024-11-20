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
/// Ignite client pool configuration.
/// </summary>
public sealed record IgniteClientPoolConfiguration
{
    /// <summary>
    /// Gets or sets the pool size (maximum number of clients).
    /// </summary>
    public int Size { get; set; }

    /// <summary>
    /// Gets or sets the configuration for pooled clients.
    /// </summary>
    public required IgniteClientConfiguration ClientConfiguration { get; set; }
}
