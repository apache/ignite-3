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

namespace Apache.Ignite.Internal.Compute.Executor;

using System;
using System.Linq;
using System.Reflection;
using System.Runtime.Loader;
using Ignite.Compute;

/// <summary>
/// Job load context.
/// </summary>
/// <param name="AssemblyLoadContext">Assembly load context.</param>
internal readonly record struct JobLoadContext(AssemblyLoadContext AssemblyLoadContext)
{
    /// <summary>
    /// Gets or creates a job delegate for the specified type name.
    /// </summary>
    /// <param name="typeName">Job type name.</param>
    /// <returns>Job execution delegate.</returns>
    public IComputeJobWrapper CreateJobWrapper(string typeName) =>
        CreateJobWrapper(typeName, AssemblyLoadContext);

    private static IComputeJobWrapper CreateJobWrapper(string typeName, AssemblyLoadContext ctx)
    {
        var jobType = LoadJobType(typeName, ctx);
        var jobInterface = FindJobInterface(typeName, jobType);

        try
        {
            var genericArgs = jobInterface.GenericTypeArguments;
            var jobWrapperType = typeof(ComputeJobWrapper<,,>).MakeGenericType(jobType, genericArgs[0], genericArgs[1]);

            return (IComputeJobWrapper)Activator.CreateInstance(jobWrapperType)!;
        }
        catch (Exception e)
        {
            if (jobType.GetConstructor(BindingFlags.Public, []) == null)
            {
                throw new InvalidOperationException($"No public parameterless constructor for job type '{typeName}'", e);
            }

            throw;
        }
    }

    private static Type LoadJobType(string typeName, AssemblyLoadContext ctx) =>
        Type.GetType(typeName, ctx.LoadFromAssemblyName, null)
        ?? throw new InvalidOperationException($"Type '{typeName}' not found in the specified deployment units.");

    // Simple lookup by name. Will throw in a case of ambiguity.
    private static Type FindJobInterface(string typeName, Type jobType) =>
        jobType.GetInterface(typeof(IComputeJob<,>).Name, ignoreCase: false) ??
        throw new InvalidOperationException($"Failed to find job interface '{typeof(IComputeJob<,>)}' in type '{typeName}'");
}
