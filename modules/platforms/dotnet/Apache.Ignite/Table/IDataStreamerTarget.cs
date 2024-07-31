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

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Internal.Common;

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
    Task StreamDataAsync(
        IAsyncEnumerable<DataStreamerItem<T>> data,
        DataStreamerOptions? options = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Streams data into the underlying table.
    /// </summary>
    /// <param name="data">Data.</param>
    /// <param name="options">Streamer options.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    Task StreamDataAsync(
        IAsyncEnumerable<T> data,
        DataStreamerOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        IgniteArgumentCheck.NotNull(data);

        return StreamDataAsync(ConvertToItems(), options, cancellationToken);

        async IAsyncEnumerable<DataStreamerItem<T>> ConvertToItems()
        {
            await foreach (var item in data.WithCancellation(cancellationToken))
            {
                yield return DataStreamerItem.Create(item);
            }
        }
    }

    /// <summary>
    /// Streams data into the underlying table with receiver that returns results.
    /// </summary>
    /// <param name="data">Data.</param>
    /// <param name="keySelector">Key selector.</param>
    /// <param name="payloadSelector">Payload selector.</param>
    /// <param name="receiver">Streamer receiver descriptor.</param>
    /// <param name="receiverArg">Receiver argument.</param>
    /// <param name="options">Streamer options.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>
    /// A <see cref="IAsyncEnumerable{T}"/> with the results from the receiver.
    /// <para />
    /// The resulting async enumerator applies back-pressure to the data source, so it should be either fully consumed
    /// or disposed to complete the streaming. Disposing the enumerator before it is fully consumed will ignore the remaining results.
    /// </returns>
    /// <typeparam name="TSource">Source item type.</typeparam>
    /// <typeparam name="TPayload">Payload type.</typeparam>
    /// <typeparam name="TArg">Argument type.</typeparam>
    /// <typeparam name="TResult">Result type.</typeparam>
    IAsyncEnumerable<TResult> StreamDataAsync<TSource, TPayload, TArg, TResult>(
        IAsyncEnumerable<TSource> data,
        Func<TSource, T> keySelector,
        Func<TSource, TPayload> payloadSelector,
        ReceiverDescriptor<TArg, TResult> receiver,
        TArg receiverArg,
        DataStreamerOptions? options = null,
        CancellationToken cancellationToken = default)
        where TPayload : notnull;

    /// <summary>
    /// Streams data into the underlying table with receiver, ignoring receiver results (if any).
    /// </summary>
    /// <param name="data">Data.</param>
    /// <param name="keySelector">Key selector.</param>
    /// <param name="payloadSelector">Payload selector.</param>
    /// <param name="receiver">Receiver descriptor.</param>
    /// <param name="receiverArg">Receiver arg.</param>
    /// <param name="options">Streamer options.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    /// <typeparam name="TSource">Source item type.</typeparam>
    /// <typeparam name="TPayload">Payload type.</typeparam>
    /// <typeparam name="TArg">Argument type.</typeparam>
    Task StreamDataAsync<TSource, TPayload, TArg>(
        IAsyncEnumerable<TSource> data,
        Func<TSource, T> keySelector,
        Func<TSource, TPayload> payloadSelector,
        ReceiverDescriptor<TArg> receiver,
        TArg receiverArg,
        DataStreamerOptions? options = null,
        CancellationToken cancellationToken = default)
        where TPayload : notnull;
}
