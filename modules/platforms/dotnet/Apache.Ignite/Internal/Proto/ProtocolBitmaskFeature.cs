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

namespace Apache.Ignite.Internal.Proto;

using System;

/// <summary>
/// Protocol bitmask features.
/// </summary>
[Flags]
internal enum ProtocolBitmaskFeature
{
    /// <summary>
    /// User attributes in handshake.
    /// </summary>
    UserAttributes = 1,

    /// <summary>
    /// Qualified name table requests.
    /// </summary>
    TableReqsUseQualifiedName = 2,

    /// <summary>
    /// Transaction direct mapping.
    /// </summary>
    TxDirectMapping = 4,

    /// <summary>
    /// Platform compute jobs (call non-Java jobs from client).
    /// </summary>
    PlatformComputeJob = 8,

    /// <summary>
    /// Platform compute executor (respond to server calls for job execution).
    /// </summary>
    PlatformComputeExecutor = 16,

    /// <summary>
    /// Streamer receiver execution options, including .NET receivers.
    /// </summary>
    StreamerReceiverExecutionOptions = 32,

    /// <summary>
    /// Partition awareness for SQL requests.
    /// </summary>
    SqlPartitionAwareness = 1 << 9,

    /// <summary>
    /// Partition awareness for SQL requests with table name in metadata.
    /// </summary>
    SqlPartitionAwarenessTableName = 1 << 16
}
