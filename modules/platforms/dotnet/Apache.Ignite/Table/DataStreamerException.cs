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
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;

/// <summary>
/// Represents an exception that is thrown during data streaming. Includes information about failed items.
/// </summary>
[Serializable]
[SuppressMessage("Naming", "CA1711:Identifiers should not have incorrect suffix", Justification = "False positive.")]
[SuppressMessage(
    "Microsoft.Design",
    "CA1032:ImplementStandardExceptionConstructors",
    Justification="Ignite exceptions use a special constructor.")]
public class DataStreamerException : IgniteException
{
    /// <summary>
    /// Initializes a new instance of the <see cref="DataStreamerException"/> class.
    /// </summary>
    /// <param name="traceId">Trace id.</param>
    /// <param name="code">Code.</param>
    /// <param name="message">Message.</param>
    /// <param name="innerException">Inner exception.</param>
    /// <param name="failedItems">Failed items.</param>
    public DataStreamerException(Guid traceId, int code, string? message, Exception innerException, ISet<object> failedItems)
        : base(traceId, code, message, innerException)
    {
        FailedItems = failedItems;
    }

    /// <summary>
    /// Gets the set of items that were not streamed to the cluster.
    /// </summary>
    public ISet<object> FailedItems { get; }

    /// <summary>
    /// Creates a new instance of <see cref="DataStreamerException"/> from the provided cause and failed items.
    /// </summary>
    /// <param name="cause">Cause.</param>
    /// <param name="failedItems">Failed items.</param>
    /// <returns>Exception.</returns>
    public static DataStreamerException Create(Exception cause, IEnumerable failedItems)
    {
        var failedItemsSet = new HashSet<object>();

        foreach (var failedItem in failedItems)
        {
            failedItemsSet.Add(failedItem);
        }

        return cause is IgniteException iex
            ? new DataStreamerException(iex.TraceId, iex.Code, iex.Message, cause, failedItemsSet)
            : new DataStreamerException(Guid.NewGuid(), ErrorGroups.Common.Internal, cause.Message, cause, failedItemsSet);
    }
}
