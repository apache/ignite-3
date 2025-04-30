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
using System.IO;
using Internal.Common;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;

internal static class AssemblyGenerator
{
    internal static void EmitClassLib(string targetFile, string code)
    {
        var assemblyName = Path.GetFileNameWithoutExtension(targetFile);
        var syntaxTree = CSharpSyntaxTree.ParseText(code);
        var references = new[]
        {
            MetadataReference.CreateFromFile(typeof(object).Assembly.Location),
            MetadataReference.CreateFromFile(typeof(IgniteClient).Assembly.Location),
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
