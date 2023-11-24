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
using System.Net;
using System.Net.Security;
using Ignite.Sql;
using Microsoft.Extensions.Logging;
using Proto;

/// <summary>
/// Source-generated logger delegates.
/// </summary>
internal static partial class LogMessages
{
    [LoggerMessage(
        Message = "Schema loaded [tableId={TableId}, schemaVersion={SchemaVersion}]",
        Level = LogLevel.Debug,
        EventId = 1001)]
    internal static partial void LogSchemaLoadedDebug(this ILogger logger, int tableId, int schemaVersion);

    [LoggerMessage(
        Message = "Retrying unmapped columns error [tableId={TableId}, schemaVersion={SchemaVersion}, message={Message}]",
        Level = LogLevel.Debug,
        EventId = 1002)]
    internal static partial void LogRetryingUnmappedColumnsErrorDebug(this ILogger logger, int tableId, int? schemaVersion, string message);

    [LoggerMessage(
        Message = "Retrying SchemaVersionMismatch error [tableId={TableId}, " +
                  "schemaVersion={SchemaVersion}, expectedSchemaVersion={ExpectedSchemaVersion}]",
        Level = LogLevel.Debug,
        EventId = 1003)]
    internal static partial void LogRetryingSchemaVersionMismatchErrorDebug(
        this ILogger logger, int tableId, int? schemaVersion, int? expectedSchemaVersion);

    [LoggerMessage(
        Message = "Executing SQL generated by LINQ provider [statement={Statement}, parameters={Parameters}]",
        Level = LogLevel.Debug,
        SkipEnabledCheck = true,
        EventId = 1004)]
    internal static partial void LogExecutingSqlStatementDebug(this ILogger logger, SqlStatement statement, string parameters);

    [LoggerMessage(
        Message = "Connection established [remoteAddress={Endpoint}]",
        Level = LogLevel.Debug,
        EventId = 1005)]
    internal static partial void LogConnectionEstablishedDebug(this ILogger logger, EndPoint? endpoint);

    [LoggerMessage(
        Message = "SSL connection established [remoteAddress={Endpoint}, cipherSuite={CipherSuite}]",
        Level = LogLevel.Debug,
        EventId = 1006)]
    internal static partial void LogSslConnectionEstablishedDebug(this ILogger logger, EndPoint? endpoint, TlsCipherSuite cipherSuite);

    [LoggerMessage(
        Message = "Handshake succeeded [remoteAddress={Endpoint}, context={Context}]",
        Level = LogLevel.Debug,
        EventId = 1007)]
    internal static partial void LogHandshakeSucceededDebug(this ILogger logger, EndPoint? endpoint, ConnectionContext context);

    [LoggerMessage(
        Message = "Failed to dispose socket after failed connection attempt: {Message}",
        Level = LogLevel.Warning,
        EventId = 1008)]
    internal static partial void LogFailedToDisposeSocketAfterFailedConnectionAttemptWarn(
        this ILogger logger, Exception ex, string message);

    [LoggerMessage(
        Message = "Connection failed before or during handshake [remoteAddress={Endpoint}]: {Message}",
        Level = LogLevel.Warning,
        EventId = 1009)]
    internal static partial void LogConnectionFailedBeforeOrDuringHandshakeWarn(
        this ILogger logger, Exception ex, EndPoint? endpoint, string message);

    [LoggerMessage(
        Message = "Server-side IdleTimeout is not set, using configured IgniteClientConfiguration.HeartbeatInterval: {Interval}",
        Level = LogLevel.Information,
        EventId = 1010)]
    internal static partial void LogServerSizeIdleTimeoutNotSetInfo(this ILogger logger, TimeSpan interval);

    [LoggerMessage(
        Message = "Server-side IdleTimeout is {ServerIdleTimeout}, " +
                  "using configured IgniteClientConfiguration.HeartbeatInterval: {ConfiguredInterval}",
        Level = LogLevel.Information,
        EventId = 1011)]
    internal static partial void LogServerSideIdleTimeoutIgnoredInfo(
        this ILogger logger, TimeSpan serverIdleTimeout, TimeSpan configuredInterval);

    [LoggerMessage(
        Message = "Server-side IdleTimeout is {ServerIdleTimeout}, " +
                  "configured IgniteClientConfiguration.HeartbeatInterval is {ConfiguredInterval}, " +
                  "which is longer than recommended IdleTimeout / 3. " +
                  "Overriding heartbeat interval with max(IdleTimeout / 3, 500ms): {RecommendedHeartbeatInterval}",
        Level = LogLevel.Warning,
        EventId = 1012)]
    internal static partial void LogServerSideIdleTimeoutOverridesConfiguredHeartbeatIntervalWarn(
        this ILogger logger, TimeSpan serverIdleTimeout, TimeSpan configuredInterval, TimeSpan recommendedHeartbeatInterval);

    [LoggerMessage(
        Message = "Sending request [requestId={RequestId}, op={Op}, remoteAddress={RemoteAddress}]",
        Level = LogLevel.Trace,
        EventId = 1013)]
    internal static partial void LogSendingRequestTrace(this ILogger logger, ClientOp op, IPEndPoint remoteAddress, long requestId);

    [LoggerMessage(
        Message = "{Message}",
        Level = LogLevel.Error,
        EventId = 1014)]
    internal static partial void LogSocketIoError(this ILogger logger, Exception? ex, string message);

    [LoggerMessage(
        Message = "{Message}",
        Level = LogLevel.Error,
        EventId = 1015)]
    internal static partial void LogHeartbeatError(this ILogger logger, Exception? ex, string message);

    [LoggerMessage(
        Message = "{Message}",
        Level = LogLevel.Error,
        EventId = 1016)]
    internal static partial void LogUnexpectedResponseIdError(this ILogger logger, Exception? ex, string message);

    [LoggerMessage(
        Message = "Partition assignment change notification received [remoteAddress={RemoteAddress}, timestamp={Timestamp}",
        Level = LogLevel.Information,
        EventId = 1017)]
    internal static partial void LogPartitionAssignmentChangeNotificationInfo(
        this ILogger logger, IPEndPoint remoteAddress, long timestamp);

    [LoggerMessage(
        Message = "Connection closed with error [remoteAddress={RemoteAddress}]: {Message}",
        Level = LogLevel.Warning,
        EventId = 1018)]
    internal static partial void LogConnectionClosedWithErrorWarn(
        this ILogger logger, Exception ex, IPEndPoint remoteAddress, string message);

    [LoggerMessage(
        Message = "Connection closed gracefully [remoteAddress={RemoteAddress}]",
        Level = LogLevel.Debug,
        EventId = 1019)]
    internal static partial void LogConnectionClosedGracefullyDebug(this ILogger logger, IPEndPoint remoteAddress);

    [LoggerMessage(
        Message = "Ignite.NET client version {Version} is starting",
        Level = LogLevel.Information,
        EventId = 1020)]
    internal static partial void LogClientStartInfo(this ILogger logger, string version);

    [LoggerMessage(
        Message = "Failed to connect to preferred node [{NodeName}]: {Message}",
        Level = LogLevel.Debug,
        EventId = 1021)]
    internal static partial void LogFailedToConnectPreferredNodeDebug(this ILogger logger, string nodeName, string message);

    [LoggerMessage(
        Message = "Error while trying to establish secondary connections: {Message}",
        Level = LogLevel.Warning,
        EventId = 1022)]
    internal static partial void LogErrorWhileEstablishingSecondaryConnectionsWarn(this ILogger logger, Exception e, string message);

    [LoggerMessage(
        Message = "Trying to establish secondary connections - awaiting {Tasks} tasks...",
        Level = LogLevel.Debug,
        EventId = 1023)]
    internal static partial void LogTryingToEstablishSecondaryConnectionsDebug(this ILogger logger, int tasks);

    [LoggerMessage(
        Message = "{Established} secondary connections established, {Failed} failed.",
        Level = LogLevel.Debug,
        EventId = 1024)]
    internal static partial void LogSecondaryConnectionsEstablishedDebug(this ILogger logger, int established, int failed);

    [LoggerMessage(
        Message = "Failed to parse host '{Host}': {Message}",
        Level = LogLevel.Debug,
        EventId = 1025)]
    internal static partial void LogFailedToParseHostDebug(this ILogger logger, Exception e, string host, string message);

    [LoggerMessage(
        Message = "{Retrying} operation [opCode={Op}, opType={OpType}, attempt={Attempt}, lastError={LastErrorMessage}]",
        Level = LogLevel.Debug,
        EventId = 1026)]
    internal static partial void LogRetryingOperationDebug(
        this ILogger logger, string retrying, int op, ClientOp opType, int attempt, string lastErrorMessage);

    [LoggerMessage(
        Message = "Received response [requestId={RequestId}, remoteAddress={RemoteAddress}]",
        Level = LogLevel.Trace,
        EventId = 1027)]
    internal static partial void LogReceivedResponseTrace(this ILogger logger, IPEndPoint remoteAddress, long requestId);
}
