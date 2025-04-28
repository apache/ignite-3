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
using System.Collections.Concurrent;
using System.Linq;
using System.Runtime.Loader;
using Ignite.Compute;

/// <summary>
/// Job load context.
/// </summary>
/// <param name="AssemblyLoadContext">Assembly load context.</param>
internal readonly record struct JobLoadContext(AssemblyLoadContext AssemblyLoadContext)
{
    private readonly ConcurrentDictionary<string, IComputeJobWrapper> _jobDelegates = new();

    /// <summary>
    /// Gets or creates a job delegate for the specified type name.
    /// </summary>
    /// <param name="typeName">Job type name.</param>
    /// <returns>Job execution delegate.</returns>
    public IComputeJobWrapper GetOrCreateJobWrapper(string typeName) =>
        _jobDelegates.GetOrAdd(typeName, static (name, ctx) => CreateJobWrapper(name, ctx), AssemblyLoadContext);

    /// <summary>
    /// Initiates an unload of this context.
    /// </summary>
    public void Unload()
    {
        _jobDelegates.Clear();

        AssemblyLoadContext.Unload();
    }

    private static IComputeJobWrapper CreateJobWrapper(string typeName, AssemblyLoadContext ctx)
    {
        var jobType = Type.GetType(typeName, ctx.LoadFromAssemblyName, null);

        if (jobType == null)
        {
            throw new InvalidOperationException($"Type '{typeName}' not found in the specified deployment units.");
        }

        var jobInterface = jobType
            .GetInterfaces()
            .FirstOrDefault(x => x.IsGenericType && x.GetGenericTypeDefinition() == typeof(IComputeJob<,>));

        if (jobInterface == null)
        {
            throw new InvalidOperationException($"Failed to find job interface '{typeof(IComputeJob<,>)}' in type '{typeName}'");
        }

        var jobWrapperType = typeof(ComputeJobWrapper<,,>)
            .MakeGenericType(jobType, jobInterface.GenericTypeArguments[0], jobInterface.GenericTypeArguments[1]);

        return (IComputeJobWrapper)Activator.CreateInstance(jobWrapperType)!;
    }
}
