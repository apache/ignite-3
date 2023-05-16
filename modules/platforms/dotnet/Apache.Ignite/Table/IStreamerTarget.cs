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

namespace Apache.Ignite.Table;

using System;
using System.Collections.Generic;
using System.Threading.Tasks;

/// <summary>
/// Data streamer target.
/// </summary>
/// <typeparam name="T">Entry type.</typeparam>
public interface IStreamerTarget<in T>
{
    /// <summary>
    /// Streams data into the table.
    /// </summary>
    /// <param name="stream">Data stream.</param>
    /// <param name="options">Options.</param>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    Task StreamDataAsync(IAsyncEnumerable<T> stream, StreamerOptions? options = null);

    /// <summary>
    /// Streams data into the table with a receiver.
    /// </summary>
    /// <param name="stream">Data stream.</param>
    /// <param name="keySelector">Key selector for partition awareness.</param>
    /// <param name="receiverClassName">Java class name of the stream receiver.</param>
    /// <param name="resultListener">Result listener.</param>
    /// <param name="options">Options.</param>
    /// <typeparam name="TItem">Item type.</typeparam>
    /// <typeparam name="TResult">Result type type.</typeparam>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    Task StreamDataAsync<TItem, TResult>(
        IAsyncEnumerable<TItem> stream,
        Func<TItem, T> keySelector,
        string receiverClassName,
        IStreamerResultListener<TResult>? resultListener,
        StreamerOptions? options = null);
}
