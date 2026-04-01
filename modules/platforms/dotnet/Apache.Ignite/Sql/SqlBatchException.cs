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

namespace Apache.Ignite.Sql;

using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;

/// <summary>
/// Subclass of <see cref="IgniteException"/> that is thrown when an error occurs during a batch update operation.
/// In addition to the information provided by <see cref="IgniteException"/>, <see cref="SqlBatchException"/> provides the update
/// counts for all commands that were executed successfully during the batch update, that is,
/// all commands that were executed before the error occurred. The order of elements in the array of update counts
/// corresponds to the order in which these commands were added to the batch.
/// </summary>
[Serializable]
[SuppressMessage(
    "Microsoft.Design",
    "CA1032:ImplementStandardExceptionConstructors",
    Justification = "Ignite exceptions use a special constructor.")]
public sealed class SqlBatchException : SqlException
{
    /// <summary>
    /// Initializes a new instance of the <see cref="SqlBatchException"/> class.
    /// </summary>
    /// <param name="traceId">Trace id.</param>
    /// <param name="code">Code.</param>
    /// <param name="message">Message.</param>
    /// <param name="innerException">Inner exception.</param>
    public SqlBatchException(Guid traceId, int code, string? message, Exception? innerException = null)
        : base(traceId, code, message, innerException)
    {
        UpdateCounters = [];
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="SqlBatchException"/> class.
    /// </summary>
    /// <param name="traceId">Trace id.</param>
    /// <param name="code">Code.</param>
    /// <param name="updateCounters">Update counters.</param>
    /// <param name="message">Message.</param>
    /// <param name="innerException">Inner exception.</param>
    public SqlBatchException(Guid traceId, int code, IReadOnlyList<long> updateCounters, string? message, Exception? innerException = null)
        : base(traceId, code, message, innerException)
    {
        UpdateCounters = updateCounters;
    }

    /// <summary>
    /// Gets the update counters. The array describes the outcome of a batch execution.
    /// Elements correspond to the order in which commands were added to the batch.
    /// Contains update counts for all commands that were executed successfully before the error occurred.
    /// </summary>
    public IReadOnlyList<long> UpdateCounters { get; }
}
