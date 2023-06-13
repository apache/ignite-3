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

// ReSharper disable TypeParameterCanBeVariant (justification: future compatibility for streamer with receiver).
namespace Apache.Ignite.Table;

using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

/// <summary>
/// Represents an entity that can be used as a target for streaming data.
/// </summary>
/// <typeparam name="T">Data type.</typeparam>
public interface IDataStreamerTarget<T>
{
    /// <summary>
    /// Streams data into the underlying table.
    /// </summary>
    /// <param name="data">Data.</param>
    /// <param name="options">Streamer options.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    Task StreamDataAsync(IAsyncEnumerable<T> data, DataStreamerOptions? options = null, CancellationToken cancellationToken = default);
}
