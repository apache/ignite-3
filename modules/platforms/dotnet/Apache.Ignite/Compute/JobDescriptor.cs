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

namespace Apache.Ignite.Compute;

using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using Marshalling;

/// <summary>
/// Compute job descriptor.
/// </summary>
/// <param name="JobClassName">Java class name of the job to execute.</param>
/// <param name="DeploymentUnits">Deployment units.</param>
/// <param name="Options">Options.</param>
/// <param name="ArgMarshaller">Argument marshaller (serializer).</param>
/// <param name="ResultMarshaller">Result marshaller (deserializer).</param>
/// <typeparam name="TArg">Argument type.</typeparam>
/// <typeparam name="TResult">Result type.</typeparam>
[SuppressMessage("StyleCop.CSharp.MaintainabilityRules", "SA1402:File may only contain a single type", Justification = "Reviewed.")]
public sealed record JobDescriptor<TArg, TResult>(
    string JobClassName,
    IEnumerable<DeploymentUnit>? DeploymentUnits = null,
    JobExecutionOptions? Options = null,
    IMarshaller<TArg>? ArgMarshaller = null,
    IMarshaller<TResult>? ResultMarshaller = null)
{
    /// <summary>
    /// Initializes a new instance of the <see cref="JobDescriptor{TArg, TResult}"/> class.
    /// </summary>
    /// <param name="type">Type implementing <see cref="IComputeJob{TArg,TResult}"/>.</param>
    /// <param name="deploymentUnits">Deployment units.</param>
    /// <param name="options">Options.</param>
    /// <param name="argMarshaller">Arg marshaller.</param>
    /// <param name="resultMarshaller">Result marshaller.</param>
    public JobDescriptor(
        Type type,
        IEnumerable<DeploymentUnit>? deploymentUnits = null,
        JobExecutionOptions? options = null,
        IMarshaller<TArg>? argMarshaller = null,
        IMarshaller<TResult>? resultMarshaller = null)
        : this(
            type.AssemblyQualifiedName ?? throw new ArgumentException("Type has null AssemblyQualifiedName: " + type),
            deploymentUnits,
            EnsureDotNetExecutor(options),
            argMarshaller,
            resultMarshaller)
    {
        // No-op.
    }

    private static JobExecutionOptions EnsureDotNetExecutor(JobExecutionOptions? options) =>
        options == null ?
            new JobExecutionOptions(ExecutorType: JobExecutorType.DotNetSidecar) :
            options with { ExecutorType = JobExecutorType.DotNetSidecar };
}

/// <summary>
/// Job descriptor factory methods for .NET jobs.
/// </summary>
public static class JobDescriptor
{
    /// <summary>
    /// Creates a job descriptor for the provided job.
    /// </summary>
    /// <param name="job">Job instance.</param>
    /// <typeparam name="TArg">Argument type.</typeparam>
    /// <typeparam name="TResult">Result type.</typeparam>
    /// <returns>Job descriptor.</returns>
    public static JobDescriptor<TArg, TResult> Of<TArg, TResult>(IComputeJob<TArg, TResult> job) =>
        new(job.GetType(), argMarshaller: job.InputMarshaller, resultMarshaller: job.ResultMarshaller);
}
