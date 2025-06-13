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
using System.Reflection;
using System.Runtime.Loader;
using Ignite.Compute;
using Ignite.Table;
using Table.StreamerReceiverExecutor;

/// <summary>
/// Job load context.
/// </summary>
/// <param name="AssemblyLoadContext">Assembly load context.</param>
internal readonly record struct JobLoadContext(AssemblyLoadContext AssemblyLoadContext) : IDisposable
{
    /// <summary>
    /// Gets or creates a job delegate for the specified type name.
    /// </summary>
    /// <param name="typeName">Job type name.</param>
    /// <returns>Job execution delegate.</returns>
    public IComputeJobWrapper CreateJobWrapper(string typeName) =>
        CreateWrapper<IComputeJobWrapper>(
            typeName, typeof(IComputeJob<,>), typeof(ComputeJobWrapper<,,>), AssemblyLoadContext);

    /// <summary>
    /// Gets or creates a receiver delegate for the specified type name.
    /// </summary>
    /// <param name="typeName">Receiver type name.</param>
    /// <returns>Receiver execution delegate.</returns>
    public IDataStreamerReceiverWrapper CreateReceiverWrapper(string typeName) =>
        CreateWrapper<IDataStreamerReceiverWrapper>(
            typeName, typeof(IDataStreamerReceiver<,,>), typeof(DataStreamerReceiverWrapper<,,,>), AssemblyLoadContext);

    /// <inheritdoc/>
    public void Dispose() => AssemblyLoadContext.Unload();

    private static T CreateWrapper<T>(string wrappedTypeName, Type openInterfaceType, Type openWrapperType, AssemblyLoadContext ctx)
    {
        var type = LoadType(wrappedTypeName, ctx);
        var closedInterfaceType = FindInterface(type, openInterfaceType);

        try
        {
            var genericArgs = closedInterfaceType.GenericTypeArguments;
            var jobWrapperType = openWrapperType.MakeGenericType([type, .. genericArgs]);

            return (T)Activator.CreateInstance(jobWrapperType)!;
        }
        catch (Exception e)
        {
            if (type.GetConstructor(BindingFlags.Public, []) == null)
            {
                throw new InvalidOperationException($"No public parameterless constructor for type '{wrappedTypeName}'", e);
            }

            throw;
        }
    }

    private static Type LoadType(string typeName, AssemblyLoadContext ctx)
    {
        try
        {
            return Type.GetType(typeName, ctx.LoadFromAssemblyName, null, throwOnError: true)
                   ?? throw new InvalidOperationException($"Type '{typeName}' not found in the specified deployment units.");
        }
        catch (Exception e)
        {
            throw new InvalidOperationException($"Failed to load type '{typeName}' from the specified deployment units: {e.Message}", e);
        }
    }

    // Simple lookup by name. Will throw in a case of ambiguity.
    private static Type FindInterface(Type type, Type interfaceType) =>
        type.GetInterface(interfaceType.Name, ignoreCase: false) ??
        throw new InvalidOperationException($"Failed to find interface '{interfaceType}' in type '{type}'");
}
