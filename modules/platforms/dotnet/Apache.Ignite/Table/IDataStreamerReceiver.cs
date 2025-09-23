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

using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Threading;
using System.Threading.Tasks;
using Marshalling;

/// <summary>
/// Data streamer receiver.
/// </summary>
/// <typeparam name="TItem">Payload item type.</typeparam>
/// <typeparam name="TArg">Argument type.</typeparam>
/// <typeparam name="TResult">Result type.</typeparam>
[SuppressMessage("ReSharper", "TypeParameterCanBeVariant", Justification = "Won't be possible later with marshallers.")]
public interface IDataStreamerReceiver<TItem, TArg, TResult>
{
    /// <summary>
    /// Gets the custom marshaller for the receiver payload.
    /// </summary>
    IMarshaller<TItem>? PayloadMarshaller => null;

    /// <summary>
    /// Gets the custom marshaller for the receiver input argument.
    /// </summary>
    IMarshaller<TArg>? ArgumentMarshaller => null;

    /// <summary>
    /// Gets the custom marshaller for the receiver result.
    /// </summary>
    IMarshaller<TResult>? ResultMarshaller => null;

    /// <summary>
    /// Receives a page of items from the data streamer.
    /// <para />
    /// The receiver is called for each page (batch) in the data streamer and is responsible for processing the items,
    /// updating zero or more tables, and returning a result.
    /// </summary>
    /// <param name="page">A page of items. Page size is controlled by <see cref="DataStreamerOptions.PageSize"/>.</param>
    /// <param name="arg">Receiver argument.</param>
    /// <param name="context">Context.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A collection of results. Can be null.</returns>
    ValueTask<IList<TResult>?> ReceiveAsync(
        IList<TItem> page,
        TArg arg,
        IDataStreamerReceiverContext context,
        CancellationToken cancellationToken);
}
