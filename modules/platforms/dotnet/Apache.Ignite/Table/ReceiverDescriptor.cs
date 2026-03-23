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
using Marshalling;

/// <summary>
/// Stream receiver descriptor without results. If the specified receiver returns results, they will be discarded on the server.
/// </summary>
/// <param name="ReceiverClassName">Name of the streamer receiver class to execute.</param>
/// <param name="DeploymentUnits">Deployment units.</param>
/// <param name="Options">Execution options.</param>
/// <typeparam name="TArg">Argument type.</typeparam>
[SuppressMessage("StyleCop.CSharp.MaintainabilityRules", "SA1402:File may only contain a single type", Justification = "Reviewed.")]
public sealed record ReceiverDescriptor<TArg>(
    string ReceiverClassName,
    IEnumerable<DeploymentUnit>? DeploymentUnits = null,
    ReceiverExecutionOptions? Options = null);

/// <summary>
/// Stream receiver descriptor with a result type.
/// </summary>
/// <param name="ReceiverClassName">Name of the streamer receiver class to execute.</param>
/// <param name="DeploymentUnits">Deployment units.</param>
/// <param name="Options">Execution options.</param>
/// <typeparam name="TArg">Argument type.</typeparam>
/// <typeparam name="TResult">Result type.</typeparam>
[SuppressMessage("StyleCop.CSharp.MaintainabilityRules", "SA1402:File may only contain a single type", Justification = "Reviewed.")]
[Obsolete("Use ReceiverDescriptor<TItem, TArg, TResult> instead. This type will be removed in a future release.")]
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
            ReceiverDescriptor.EnsureDotNetExecutor(options))
    {
        // No-op.
    }
}

/// <summary>
/// Stream receiver descriptor with a result type.
/// </summary>
/// <param name="ReceiverClassName">Name of the streamer receiver class to execute.</param>
/// <param name="DeploymentUnits">Deployment units.</param>
/// <param name="Options">Execution options.</param>
/// <param name="PayloadMarshaller">Payload marshaller.</param>
/// <param name="ArgumentMarshaller">Argument marshaller.</param>
/// <param name="ResultMarshaller">Result marshaller.</param>
/// <typeparam name="TItem">Streamer item type.</typeparam>
/// <typeparam name="TArg">Argument type.</typeparam>
/// <typeparam name="TResult">Result type.</typeparam>
[SuppressMessage("StyleCop.CSharp.MaintainabilityRules", "SA1402:File may only contain a single type", Justification = "Reviewed.")]
public sealed record ReceiverDescriptor<TItem, TArg, TResult>(
    string ReceiverClassName,
    IEnumerable<DeploymentUnit>? DeploymentUnits = null,
    ReceiverExecutionOptions? Options = null,
    IMarshaller<TItem>? PayloadMarshaller = null,
    IMarshaller<TArg>? ArgumentMarshaller = null,
    IMarshaller<TResult>? ResultMarshaller = null)
{
    /// <summary>
    /// Initializes a new instance of the <see cref="ReceiverDescriptor{TItem,TArg,TResult}"/> class.
    /// </summary>
    /// <param name="type">Receiver type.</param>
    /// <param name="deploymentUnits">Deployment units.</param>
    /// <param name="options">Options.</param>
    /// <param name="payloadMarshaller">Payload marshaller.</param>
    /// <param name="argumentMarshaller">Argument marshaller.</param>
    /// <param name="resultMarshaller">Result marshaller.</param>
    public ReceiverDescriptor(
        Type type,
        IEnumerable<DeploymentUnit>? deploymentUnits = null,
        ReceiverExecutionOptions? options = null,
        IMarshaller<TItem>? payloadMarshaller = null,
        IMarshaller<TArg>? argumentMarshaller = null,
        IMarshaller<TResult>? resultMarshaller = null)
        : this(
            type.AssemblyQualifiedName ?? throw new ArgumentException("Type has null AssemblyQualifiedName: " + type),
            deploymentUnits,
            ReceiverDescriptor.EnsureDotNetExecutor(options),
            payloadMarshaller,
            argumentMarshaller,
            resultMarshaller)
    {
        // No-op.
    }
}

/// <summary>
/// Receiver descriptor factory methods for .NET receivers.
/// </summary>
public static class ReceiverDescriptor
{
    /// <summary>
    /// Creates a descriptor for the provided streamer receiver.
    /// </summary>
    /// <param name="receiver">Receiver instance.</param>
    /// <typeparam name="TItem">Item type.</typeparam>
    /// <typeparam name="TArg">Argument type.</typeparam>
    /// <typeparam name="TResult">Result type.</typeparam>
    /// <returns>Receiver descriptor.</returns>
    public static ReceiverDescriptor<TItem, TArg, TResult> Of<TItem, TArg, TResult>(IDataStreamerReceiver<TItem, TArg, TResult> receiver) =>
        new(
            receiver.GetType(),
            payloadMarshaller: receiver.PayloadMarshaller,
            argumentMarshaller: receiver.ArgumentMarshaller,
            resultMarshaller: receiver.ResultMarshaller);

    /// <summary>
    /// Ensures that the provided <see cref="ReceiverExecutionOptions"/> is set to use the .NET executor.
    /// </summary>
    /// <param name="options">Options.</param>
    /// <returns>Receiver execution options with .NET executor type.</returns>
    internal static ReceiverExecutionOptions EnsureDotNetExecutor(ReceiverExecutionOptions? options) =>
        options == null ?
            new ReceiverExecutionOptions(ExecutorType: JobExecutorType.DotNetSidecar) :
            options with { ExecutorType = JobExecutorType.DotNetSidecar };
}
