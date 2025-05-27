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

namespace Apache.Ignite.Tests.Compute.Executor;

using System.Diagnostics.CodeAnalysis;
using System.IO;
using TestHelpers;

/// <summary>
/// Test job generator. Produces temporary assemblies with job classes.
/// </summary>
[SuppressMessage("StyleCop.CSharp.ReadabilityRules", "SA1118:Parameter should not span multiple lines", Justification = "Tests")]
public static class JobGenerator
{
    public static string EmitEchoJob(TempDir tempDir, string asmName) =>
        EmitJob(
            tempDir,
            asmName,
            """
            public class EchoJob : IComputeJob<string, string>
            {
                public ValueTask<string> ExecuteAsync(IJobExecutionContext context, string arg, CancellationToken cancellationToken) =>
                    ValueTask.FromResult("Echo: " + arg);
            }
            """);

    public static string EmitGetAndSetStaticFieldJob(TempDir tempDir, string asmName) =>
        EmitJob(
            tempDir,
            asmName,
            """
            public class GetAndSetStaticFieldJob : IComputeJob<string, string>
            {
                public static string StaticField { get; set; } = "Initial";
                
                public ValueTask<string> ExecuteAsync(IJobExecutionContext context, string arg, CancellationToken cancellationToken)
                {
                    var oldValue = StaticField;
                    StaticField = arg;
                    
                    return ValueTask.FromResult(oldValue);
                }
                    
            }
            """);

    public static string EmitReturnConstJob(TempDir tempDir, string asmName, string jobResult) =>
        EmitJob(
            tempDir,
            asmName,
            $$"""
            public class ReturnConstJob : IComputeJob<string, string>
            {
                public ValueTask<string> ExecuteAsync(IJobExecutionContext context, string arg, CancellationToken cancellationToken) =>
                    ValueTask.FromResult("{{jobResult}}");
                    
            }
            """);

    public static string EmitGetReferencedIgniteAssemblyJob(TempDir tempDir, string asmName, string? igniteDllPath = null) =>
        EmitJob(
            tempDir,
            asmName,
            """
            public class GetReferencedIgniteAssemblyJob : IComputeJob<string, string>
            {
                public ValueTask<string> ExecuteAsync(IJobExecutionContext context, string arg, CancellationToken cancellationToken)
                {
                    foreach (var asm in Assembly.GetExecutingAssembly().GetReferencedAssemblies())
                    {
                        if (asm.FullName.Contains("Apache.Ignite", StringComparison.Ordinal))
                        {
                            return ValueTask.FromResult(asm.FullName);
                        }
                    }
                    
                    return ValueTask.FromResult(string.Empty);
                }
            }
            """,
            igniteDllPath);

    public static string EmitJob(TempDir tempDir, string asmName, [StringSyntax("C#")] string jobCode, string? igniteDllPath = null)
    {
        var targetFile = Path.Combine(tempDir.Path, $"{asmName}.dll");
        igniteDllPath ??= typeof(IgniteClient).Assembly.Location;

        AssemblyGenerator.EmitClassLib(
            targetFile,
            $$"""
              using System;
              using System.Reflection;
              using System.Threading;
              using System.Threading.Tasks;
              using Apache.Ignite.Compute;

              namespace TestNamespace
              {
                  {{jobCode}}
              }
              """,
            igniteDllPath);

        return targetFile;
    }
}
