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
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using Compute;

/// <summary>
/// Stream receiver descriptor without results. If the specified receiver returns results, they will be discarded on the server.
/// </summary>
/// <param name="ReceiverClassName">Java class name of the streamer receiver to execute.</param>
/// <param name="DeploymentUnits">Deployment units.</param>
/// <param name="Options">Execution options.</param>
/// <typeparam name="TArg">Argument type.</typeparam>
[SuppressMessage("StyleCop.CSharp.MaintainabilityRules", "SA1402:File may only contain a single type", Justification = "Reviewed.")]
public sealed record ReceiverDescriptor<TArg>(
    string ReceiverClassName,
    IEnumerable<DeploymentUnit>? DeploymentUnits = null,
    ReceiverExecutionOptions? Options = null);

/// <summary>
/// Stream receiver descriptor with result type.
/// </summary>
/// <param name="ReceiverClassName">Java class name of the streamer receiver to execute.</param>
/// <param name="DeploymentUnits">Deployment units.</param>
/// <param name="Options">Execution options.</param>
/// <typeparam name="TArg">Argument type.</typeparam>
/// <typeparam name="TResult">Result type.</typeparam>
[SuppressMessage("StyleCop.CSharp.MaintainabilityRules", "SA1402:File may only contain a single type", Justification = "Reviewed.")]
public sealed record ReceiverDescriptor<TArg, TResult>(
    string ReceiverClassName,
    IEnumerable<DeploymentUnit>? DeploymentUnits = null,
    ReceiverExecutionOptions? Options = null)
{
    /// <summary>
    /// Initializes a new instance of the <see cref="ReceiverDescriptor{TArg,TResult}"/> class.
    /// </summary>
    /// <param name="type">Receiver type.</param>
    /// <param name="deploymentUnits">Deployment units.</param>
    /// <param name="options">Options.</param>
    public ReceiverDescriptor(
        Type type,
        IEnumerable<DeploymentUnit>? deploymentUnits = null,
        ReceiverExecutionOptions? options = null)
        : this(
            type.AssemblyQualifiedName ?? throw new ArgumentException("Type has null AssemblyQualifiedName: " + type),
            deploymentUnits,
            EnsureDotNetExecutor(options))
    {
        // No-op.
    }

    private static ReceiverExecutionOptions EnsureDotNetExecutor(ReceiverExecutionOptions? options) =>
        options == null ?
            new ReceiverExecutionOptions(ExecutorType: JobExecutorType.DotNetSidecar) :
            options with { ExecutorType = JobExecutorType.DotNetSidecar };
}

/// <summary>
/// Receiver descriptor factory methods for .NET receivers.
/// </summary>
public static class ReceiverDescriptor
{
    /// <summary>
    /// Creates a receiver descriptor for the provided job.
    /// </summary>
    /// <param name="receiver">Receiver instance.</param>
    /// <typeparam name="TItem">Item type.</typeparam>
    /// <typeparam name="TArg">Argument type.</typeparam>
    /// <typeparam name="TResult">Result type.</typeparam>
    /// <returns>Receiver descriptor.</returns>
    public static ReceiverDescriptor<TArg, TResult> Of<TItem, TArg, TResult>(IDataStreamerReceiver<TItem, TArg, TResult> receiver) =>
        new(receiver.GetType());
}
