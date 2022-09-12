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

namespace Apache.Ignite.Internal.Generators
{
    using System;
    using System.Collections.Immutable;
    using System.IO;
    using System.Linq;
    using System.Reflection;
    using System.Text.RegularExpressions;
    using Microsoft.CodeAnalysis;

    /// <summary>
    /// Generates exception classes from Java exceptions.
    /// </summary>
    [Generator]
    public class ExceptionsGenerator : ISourceGenerator
    {
        /// <inheritdoc/>
        public void Initialize(GeneratorInitializationContext context)
        {
            // No-op.
        }

        /// <inheritdoc/>
        public void Execute(GeneratorExecutionContext context)
        {
            var javaModulesDirectory = context.GetJavaModulesDirectory();

            var javaExceptionsWithParents = Directory.EnumerateFiles(
                    javaModulesDirectory,
                    "*Exception.java",
                    SearchOption.AllDirectories)
                .Where(x => !x.Contains("internal"))
                .Select(File.ReadAllText)
                .Select(x => Regex.Match(x, @"public class (\w+) extends (\w+)"))
                .Where(x => x.Success && !x.Value.Contains("RaftException")) // Ignore duplicate RaftException.
                .Where(x => !x.Value.Contains("IgniteClient")) // Skip Java client exceptions.
                .ToDictionary(x => x.Groups[1].Value, x => x.Groups[2].Value);

            var existingExceptions = context.Compilation.SyntaxTrees
                .Where(x => x.FilePath.Contains("Exception"))
                .Select(x => Path.GetFileNameWithoutExtension(x.FilePath))
                .ToImmutableHashSet();

            var javaExceptions = javaExceptionsWithParents
                .Select(x => x.Key)
                .Where(x => !existingExceptions.Contains(x))
                .Where(IsIgniteException)
                .ToList();

            if (javaExceptionsWithParents.Count == 0 || javaExceptions.Count == 0)
            {
                throw new Exception($"Failed to detect Java exception classes in {javaModulesDirectory}.");
            }

            var template = GetExceptionClassTemplate();

            foreach (var javaException in javaExceptions)
            {
                // TODO: Put into correct namespace?
                var xmlDoc = Regex.Replace(javaException, "[A-Z]", " $1");

                var src = template
                    .Replace("IgniteTemplateException", javaException)
                    .Replace(" XMLDOC", xmlDoc);

                context.AddSource(javaException + ".g.cs", src);
            }

            bool IsIgniteException(string? ex) =>
                ex != null &&
                (ex == "IgniteException" ||
                 IsIgniteException(javaExceptionsWithParents.TryGetValue(ex, out var parent) ? parent : null));
        }

        private static string GetExceptionClassTemplate()
        {
            using var stream = Assembly.GetExecutingAssembly().GetManifestResourceStream(
                "Apache.Ignite.Internal.Generators.ExceptionTemplate.cs");

            using var reader = new StreamReader(stream!);

            return reader.ReadToEnd();
        }
    }
}
