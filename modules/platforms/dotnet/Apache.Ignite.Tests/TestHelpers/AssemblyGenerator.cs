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

namespace Apache.Ignite.Tests.TestHelpers;

using System;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using Internal.Common;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;

internal static class AssemblyGenerator
{
    internal static void EmitClassLib(string targetFile, [StringSyntax("C#")] string code, string referencePath)
    {
        var assemblyName = Path.GetFileNameWithoutExtension(targetFile);
        var syntaxTree = CSharpSyntaxTree.ParseText(code);

        var refDir = Path.GetDirectoryName(typeof(object).Assembly.Location)!;

        var references = new[]
        {
            MetadataReference.CreateFromFile(typeof(object).Assembly.Location),
            MetadataReference.CreateFromFile(Path.Combine(refDir, "System.dll")),
            MetadataReference.CreateFromFile(Path.Combine(refDir, "System.Runtime.dll")),
            MetadataReference.CreateFromFile(referencePath)
        };

        var compilation = CSharpCompilation.Create(
            assemblyName,
            [syntaxTree],
            references,
            options: new(OutputKind.DynamicallyLinkedLibrary));

        using var stream = new FileStream(targetFile, FileMode.Create);

        var result = compilation.Emit(stream);
        if (!result.Success)
        {
            throw new InvalidOperationException($"Compilation failed: {result.Diagnostics.StringJoin()}");
        }
    }
}
