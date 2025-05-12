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

namespace Apache.Ignite.Tests.Table;

using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Threading;
using System.Threading.Tasks;
using Ignite.Table;

/// <summary>
/// .NET streamer receivers.
/// </summary>
[SuppressMessage("Design", "CA1034:Nested types should not be visible", Justification = "Tests.")]
public static class DotNetReceivers
{
    public static readonly ReceiverDescriptor<object, object> Echo = ReceiverDescriptor.Of(new EchoReceiver());

    public static readonly ReceiverDescriptor<object, object> EchoArgs = ReceiverDescriptor.Of(new EchoArgsReceiver());

    public static readonly ReceiverDescriptor<object, object> Error = ReceiverDescriptor.Of(new ErrorReceiver());

    public class EchoReceiver : IDataStreamerReceiver<object, object, object>
    {
        public ValueTask<IList<object>?> ReceiveAsync(
            IList<object> page,
            IDataStreamerReceiverContext context,
            object? arg,
            CancellationToken cancellationToken) =>
            ValueTask.FromResult(page)!;
    }

    public class EchoArgsReceiver : IDataStreamerReceiver<object, object, object>
    {
        public ValueTask<IList<object>?> ReceiveAsync(
            IList<object> page,
            IDataStreamerReceiverContext context,
            object? arg,
            CancellationToken cancellationToken) =>
            ValueTask.FromResult<IList<object>?>([arg!]);
    }

    public class ErrorReceiver : IDataStreamerReceiver<object, object, object>
    {
        public async ValueTask<IList<object>?> ReceiveAsync(IList<object> page, IDataStreamerReceiverContext context, object? arg, CancellationToken cancellationToken)
        {
            await Task.Delay(1, cancellationToken);

            throw new IgniteException(Guid.NewGuid(), ErrorGroups.Catalog.Validation, $"Error in receiver: {arg}");
        }
    }
}
