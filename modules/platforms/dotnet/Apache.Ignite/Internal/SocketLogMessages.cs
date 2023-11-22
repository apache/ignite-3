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

namespace Apache.Ignite.Internal;

using System;
using Microsoft.Extensions.Logging;
using Proto;

/// <summary>
/// Source-generated socket log messages.
/// </summary>
internal static partial class SocketLogMessages
{
    [LoggerMessage(Message = "Ignite.NET client version {Version} is starting", Level = LogLevel.Information)]
    internal static partial void LogClientStartInfo(this ILogger logger, string version);

    [LoggerMessage(Message = "Failed to connect to preferred node [{NodeName}]: {Message}", Level = LogLevel.Debug)]
    internal static partial void LogFailedToConnectPreferredNodeDebug(this ILogger logger, string nodeName, string message);

    [LoggerMessage(Message = "Error while trying to establish secondary connections: {Message}", Level = LogLevel.Warning)]
    internal static partial void LogErrorWhileEstablishingSecondaryConnectionsWarn(this ILogger logger, Exception e, string message);

    [LoggerMessage(Message = "Trying to establish secondary connections - awaiting {Tasks} tasks...", Level = LogLevel.Debug)]
    internal static partial void LogTryingToEstablishSecondaryConnectionsDebug(this ILogger logger, int tasks);

    [LoggerMessage(Message = "{Established} secondary connections established, {Failed} failed.", Level = LogLevel.Debug)]
    internal static partial void LogSecondaryConnectionsEstablishedDebug(this ILogger logger, int established, int failed);

    [LoggerMessage(Message = "Failed to parse host '{Host}': {Message}", Level = LogLevel.Debug)]
    internal static partial void LogFailedToParseHostDebug(this ILogger logger, Exception e, string host, string message);

    // _logger.LogDebug($"Not retrying operation [opCode={(int)op}, opType={op}, attempt={attempt}, lastError={exception}]");
    // _logger.LogDebug($"Retrying operation [opCode={(int)op}, opType={op}, attempt={attempt}, lastError={exception}]");
    [LoggerMessage(
        Message = "{Retrying} operation [opCode={Op}, opType={OpType}, attempt={Attempt}, lastError={LastErrorMessage}]",
        Level = LogLevel.Debug)]
    internal static partial void LogRetryingOperationDebug(
        this ILogger logger, string retrying, int op, ClientOp opType, int attempt, string lastErrorMessage);
}
