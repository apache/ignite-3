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

using System.Collections.Generic;
using System.Reflection;
using System.Runtime.Loader;

/// <summary>
/// Loader for deployment units in Apache Ignite compute execution context.
/// </summary>
internal static class DeploymentUnitLoader
{
    private static readonly Assembly IgniteAssembly = typeof(DeploymentUnitLoader).Assembly;

    private static readonly string IgniteAssemblyName = IgniteAssembly.GetName().Name!;

    /// <summary>
    /// Creates a new job load context for the specified deployment unit paths.
    /// </summary>
    /// <param name="paths">The deployment unit paths to load assemblies from.</param>
    /// <returns>A new job load context instance.</returns>
    public static JobLoadContext GetJobLoadContext(DeploymentUnitPaths paths)
    {
        var asmCtx = new AssemblyLoadContext(name: null, isCollectible: true);

        asmCtx.Resolving += (ctx, asmName) => ResolveAssembly(paths.Paths, asmName, ctx);

        return new JobLoadContext(asmCtx);
    }

    private static Assembly? ResolveAssembly(IReadOnlyList<string> paths, AssemblyName name, AssemblyLoadContext ctx)
    {
        if (name.Name == IgniteAssemblyName)
        {
            // Compute job might be built against a different version of Ignite, so we end up here.
            // Redirect to the current Ignite assembly.
            return IgniteAssembly;
        }

        foreach (var path in paths)
        {
            var dllName = $"{name.Name}.dll";
            var assemblyPath = System.IO.Path.Combine(path, dllName);

            if (System.IO.File.Exists(assemblyPath))
            {
                return ctx.LoadFromAssemblyPath(assemblyPath);
            }
        }

        return null;
    }
}
