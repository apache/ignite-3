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

namespace Apache.Ignite.Internal.Table;

using Microsoft.Extensions.Logging;

/// <summary>
/// Source-generated log messages for <see cref="Table"/> and related classes.
/// </summary>
internal static partial class TableLogMessages
{
    [LoggerMessage(Message = "Schema loaded [tableId={TableId}, schemaVersion={SchemaVersion}]", Level = LogLevel.Debug)]
    internal static partial void LogSchemaLoadedDebug(this ILogger logger, int tableId, int schemaVersion);

    [LoggerMessage(
        Message = "Retrying unmapped columns error [tableId={TableId}, schemaVersion={SchemaVersion}, message={Message}]",
        Level = LogLevel.Debug)]
    internal static partial void LogRetryingUnmappedColumnsErrorDebug(this ILogger logger, int tableId, int? schemaVersion, string message);

    [LoggerMessage(
        Message = "Retrying SchemaVersionMismatch error [tableId={TableId}, " +
                  "schemaVersion={SchemaVersion}, expectedSchemaVersion={ExpectedSchemaVersion}]",
        Level = LogLevel.Debug)]
    internal static partial void LogRetryingSchemaVersionMismatchErrorDebug(
        this ILogger logger, int tableId, int? schemaVersion, int? expectedSchemaVersion);
}
