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
using Microsoft.Extensions.Logging;
using Proto;

/// <summary>
/// Source-generated log messages for <see cref="ClientSocket"/>.
/// </summary>
internal static partial class ClientSocketLogMessages
{
    [LoggerMessage(Message = "Connection established [remoteAddress={Endpoint}]", Level = LogLevel.Debug)]
    internal static partial void LogConnectionEstablishedDebug(this ILogger logger, EndPoint? endpoint);

    [LoggerMessage(Message = "SSL connection established [remoteAddress={Endpoint}, cipherSuite={CipherSuite}]", Level = LogLevel.Debug)]
    internal static partial void LogSslConnectionEstablishedDebug(this ILogger logger, EndPoint? endpoint, TlsCipherSuite cipherSuite);

    [LoggerMessage(Message = "Handshake succeeded [remoteAddress={Endpoint}, context={Context}]", Level = LogLevel.Debug)]
    internal static partial void LogHandshakeSucceededDebug(this ILogger logger, EndPoint? endpoint, ConnectionContext context);

    [LoggerMessage(Message = "Failed to dispose socket after failed connection attempt: {Message}", Level = LogLevel.Warning)]
    internal static partial void LogFailedToDisposeSocketAfterFailedConnectionAttemptWarn(
        this ILogger logger, Exception ex, string message);

    [LoggerMessage(
        Message = "Connection failed before or during handshake [remoteAddress={Endpoint}]: {Message}",
        Level = LogLevel.Warning)]
    internal static partial void LogConnectionFailedBeforeOrDuringHandshakeWarn(
        this ILogger logger, Exception ex, EndPoint? endpoint, string message);

    [LoggerMessage(
        Message = "Server-side IdleTimeout is not set, using configured IgniteClientConfiguration.HeartbeatInterval: {Interval}",
        Level = LogLevel.Information)]
    internal static partial void LogServerSizeIdleTimeoutNotSetInfo(this ILogger logger, TimeSpan interval);

    [LoggerMessage(
        Message = "Server-side IdleTimeout is {ServerIdleTimeout}, " +
                  "using configured IgniteClientConfiguration.HeartbeatInterval: {ConfiguredInterval}",
        Level = LogLevel.Information)]
    internal static partial void LogServerSideIdleTimeoutIgnoredInfo(
        this ILogger logger, TimeSpan serverIdleTimeout, TimeSpan configuredInterval);

    [LoggerMessage(
        Message = "Server-side IdleTimeout is {ServerIdleTimeout}, " +
                  "configured IgniteClientConfiguration.HeartbeatInterval is {ConfiguredInterval}, " +
                  "which is longer than recommended IdleTimeout / 3. " +
                  "Overriding heartbeat interval with max(IdleTimeout / 3, 500ms): {RecommendedHeartbeatInterval}",
        Level = LogLevel.Information)]
    internal static partial void LogServerSideIdleTimeoutOverridesConfiguredHeartbeatIntervalInfo(
        this ILogger logger, TimeSpan serverIdleTimeout, TimeSpan configuredInterval, TimeSpan recommendedHeartbeatInterval);

    [LoggerMessage(Message = "Sending request [op={Op}, remoteAddress={RemoteAddress}, requestId={RequestId}]", Level = LogLevel.Trace)]
    internal static partial void LogSendingRequestTrace(this ILogger logger, ClientOp op, IPEndPoint remoteAddress, long requestId);

    [LoggerMessage(Message = "{Message}", Level = LogLevel.Error)]
    internal static partial void LogErrorMessage(this ILogger logger, Exception? ex, string message);

    [LoggerMessage(
        Message = "Partition assignment change notification received [remoteAddress={RemoteAddress}, timestamp={Timestamp}",
        Level = LogLevel.Information)]
    internal static partial void LogPartitionAssignmentChangeNotificationInfo(
        this ILogger logger, IPEndPoint remoteAddress, long timestamp);

    [LoggerMessage(Message = "Connection closed with error [remoteAddress={RemoteAddress}]: {Message}", Level = LogLevel.Warning)]
    internal static partial void LogConnectionClosedWithErrorWarn(
        this ILogger logger, Exception ex, IPEndPoint remoteAddress, string message);

    [LoggerMessage(Message = "Connection closed gracefully [remoteAddress={RemoteAddress}]", Level = LogLevel.Debug)]
    internal static partial void LogConnectionClosedGracefullyDebug(this ILogger logger, IPEndPoint remoteAddress);
}
