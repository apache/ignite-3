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
using System.Runtime.Loader;

/// <summary>
/// Job load context.
/// </summary>
/// <param name="AssemblyLoadContext">Assembly load context.</param>
internal readonly record struct JobLoadContext(AssemblyLoadContext AssemblyLoadContext)
{
    private readonly ConcurrentDictionary<string, JobDelegate> _jobDelegates = new();

    /// <summary>
    /// Gets or creates a job delegate for the specified type name.
    /// </summary>
    /// <param name="typeName">Job type name.</param>
    /// <returns>Job execution delegate.</returns>
    public JobDelegate GetOrCreateJobDelegate(string typeName) =>
        _jobDelegates.GetOrAdd(typeName, static (name, ctx) => CreateJobDelegate(name, ctx), AssemblyLoadContext);

    /// <summary>
    /// Initiates an unload of this context.
    /// </summary>
    public void Unload() => AssemblyLoadContext.Unload();

    private static JobDelegate CreateJobDelegate(string typeName, AssemblyLoadContext ctx)
    {
        var jobType = Type.GetType(typeName, ctx.LoadFromAssemblyName, null);

        // TODO
        return null!;
    }
}
