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

namespace Apache.Ignite.Internal.Common;

using System;
using System.Diagnostics;
using Proto;

/// <summary>
/// Exception extensions.
/// </summary>
internal static class ExceptionExtensions
{
    /// <summary>
    /// Gets expected schema version from the error data.
    /// </summary>
    /// <param name="e">Exception.</param>
    /// <returns>Expected schema version.</returns>
    /// <exception cref="IgniteException">When specified exception does not have schema version in <see cref="Exception.Data"/>.</exception>
    public static int GetExpectedSchemaVersion(this IgniteException e)
    {
        Debug.Assert(
            e.Code == ErrorGroups.Table.SchemaVersionMismatch,
            $"e.Code == ErrorGroups.Table.SchemaVersionMismatch: expected {ErrorGroups.Table.SchemaVersionMismatch}, actual {e.Code}");

        if (e.Data[ErrorExtensions.ExpectedSchemaVersion] is int schemaVer)
        {
            return schemaVer;
        }

        throw new IgniteException(
            e.TraceId,
            ErrorGroups.Client.Protocol,
            "Expected schema version is not specified in error extension map.",
            e);
    }
}
