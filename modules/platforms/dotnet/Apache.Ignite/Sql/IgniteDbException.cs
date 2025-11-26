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

using System.Data.Common;

/// <summary>
/// Ignite database exception.
/// </summary>
public sealed class IgniteDbException : DbException
{
    /// <summary>
    /// Initializes a new instance of the <see cref="IgniteDbException"/> class.
    /// </summary>
    /// <param name="message">Message.</param>
    /// <param name="innerException">Inner exception.</param>
    public IgniteDbException(string message, System.Exception innerException)
        : base(message, innerException)
    {
        // No-op.
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="IgniteDbException"/> class.
    /// </summary>
    /// <param name="message">Message.</param>
    public IgniteDbException(string message)
        : base(message)
    {
        // No-op.
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="IgniteDbException"/> class.
    /// </summary>
    public IgniteDbException()
    {
        // No-op.
    }
}
