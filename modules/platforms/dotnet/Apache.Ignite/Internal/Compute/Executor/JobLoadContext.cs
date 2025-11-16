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
using System.IO;
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
    private readonly ConcurrentDictionary<(string TypeName, Type OpenInterfaceType), (Type Type, Type ClosedWrapperType)> _typeCache = new();

    /// <summary>
    /// Gets or creates a job delegate for the specified type name.
    /// </summary>
    /// <param name="typeName">Job type name.</param>
    /// <returns>Job execution delegate.</returns>
    public IComputeJobWrapper CreateJobWrapper(string typeName) =>
        CreateWrapper<IComputeJobWrapper>(
            typeName, typeof(IComputeJob<,>), typeof(ComputeJobWrapper<,,>));

    /// <summary>
    /// Gets or creates a receiver delegate for the specified type name.
    /// </summary>
    /// <param name="typeName">Receiver type name.</param>
    /// <returns>Receiver execution delegate.</returns>
    public IDataStreamerReceiverWrapper CreateReceiverWrapper(string typeName) =>
        CreateWrapper<IDataStreamerReceiverWrapper>(
            typeName, typeof(IDataStreamerReceiver<,,>), typeof(DataStreamerReceiverWrapper<,,,>));

    /// <inheritdoc/>
    public void Dispose() => AssemblyLoadContext.Unload();

    private T CreateWrapper<T>(string wrappedTypeName, Type openInterfaceType, Type openWrapperType)
    {
        var (type, closedWrapperType) = _typeCache.GetOrAdd(
            key: (wrappedTypeName, openInterfaceType),
            valueFactory: static (key, arg) =>
                GetClosedWrapperType(key.TypeName, key.OpenInterfaceType, arg.openWrapperType, arg.AssemblyLoadContext),
            factoryArgument: (openWrapperType, AssemblyLoadContext));

        try
        {
            return (T)Activator.CreateInstance(closedWrapperType)!;
        }
        catch (Exception e)
        {
            CheckPublicCtor(type, e);
            throw;
        }
    }

    private static void CheckPublicCtor(Type type, Exception e)
    {
        if (type.GetConstructor(BindingFlags.Public, []) == null)
        {
            throw new InvalidOperationException($"No public parameterless constructor for type '{type.AssemblyQualifiedName}'", e);
        }
    }

    private static (Type Type, Type ClosedWrapperType) GetClosedWrapperType(
        string typeName, Type openInterfaceType, Type openWrapperType, AssemblyLoadContext ctx)
    {
        var type = LoadType(typeName, ctx);
        var closedInterfaceType = FindInterface(type, openInterfaceType);

        try
        {
            var genericArgs = closedInterfaceType.GenericTypeArguments;
            var closedWrapperType = openWrapperType.MakeGenericType([type, .. genericArgs]);

            return (type, closedWrapperType);
        }
        catch (Exception e)
        {
            CheckPublicCtor(type, e);
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
            if (e is FileNotFoundException fe)
            {
                CheckRuntimeVersions(typeName, fe.FileName);
            }

            throw new InvalidOperationException($"Failed to load type '{typeName}' from the specified deployment units: {e.Message}", e);
        }
    }

    private static void CheckRuntimeVersions(string typeName, string? fileName)
    {
        if (fileName == null || !fileName.StartsWith("System.", StringComparison.Ordinal))
        {
            return;
        }

        // System assembly failed to load - potentially due to runtime version mismatch.
        if (TryParseAssemblyName(fileName) is not { } assemblyName)
        {
            return;
        }

        int? requestedRuntimeVersion = assemblyName.Version?.Major;
        int? currentRuntimeVersion = typeof(object).Assembly.GetName().Version?.Major;

        if (requestedRuntimeVersion > currentRuntimeVersion)
        {
            throw new InvalidOperationException(
                $"Failed to load type '{typeName}' because it depends on a newer .NET runtime version " +
                $"(required: {requestedRuntimeVersion}, current: {currentRuntimeVersion}, missing assembly: {assemblyName}). " +
                $"Either target .NET {currentRuntimeVersion} when building the job assembly, " +
                $"or use .NET {requestedRuntimeVersion} on servers to run the job executor.");
        }
    }

    private static AssemblyName? TryParseAssemblyName(string assemblyName)
    {
        try
        {
            return new AssemblyName(assemblyName);
        }
        catch (FileLoadException)
        {
            return null;
        }
    }

    // Simple lookup by name. Will throw in a case of ambiguity.
    private static Type FindInterface(Type type, Type interfaceType) =>
        type.GetInterface(interfaceType.Name, ignoreCase: false) ??
        throw new InvalidOperationException($"Failed to find interface '{interfaceType}' in type '{type}'");
}
