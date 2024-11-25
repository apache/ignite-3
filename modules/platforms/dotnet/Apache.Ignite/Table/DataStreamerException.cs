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
[SuppressMessage("Naming", "CA1711:Identifiers should not have incorrect suffix", Justification = "False positive.")]
public partial class DataStreamerException
{
    /// <summary>
    /// Gets the set of items that were not streamed to the cluster.
    /// </summary>
    public ISet<object> FailedItems { get; private set; } = new HashSet<object>();

    /// <summary>
    /// Creates a new instance of <see cref="DataStreamerException"/> from the provided cause and failed items.
    /// </summary>
    /// <param name="cause">Cause.</param>
    /// <param name="failedItems">Failed items.</param>
    /// <returns>Exception.</returns>
    public static DataStreamerException Create(Exception cause, IEnumerable failedItems)
    {
        var ex = cause is IgniteException iex
            ? new DataStreamerException(iex.TraceId, iex.Code, iex.Message, iex.InnerException)
            : new DataStreamerException(Guid.NewGuid(), 0, cause.Message, cause);

        foreach (var failedItem in failedItems)
        {
            ex.FailedItems.Add(failedItem);
        }

        return ex;
    }
}
