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

namespace Apache.Ignite.Tests
{
    using System;
    using System.Collections.Generic;
    using System.Collections.Immutable;
    using System.IO;
    using System.Linq;
    using System.Text.RegularExpressions;
    using NUnit.Framework;

    /// <summary>
    /// Tests Ignite exceptions.
    /// </summary>
    public class ExceptionsTests
    {
        [Test]
        public void TestAllJavaIgniteExceptionsHaveDotNetCounterparts()
        {
            var modulesDir = Path.GetFullPath(Path.Combine(TestUtils.RepoRootDir, "modules"));

            var javaExceptionsWithParents = Directory.EnumerateFiles(modulesDir, "*Exception.java", SearchOption.AllDirectories)
                .Where(x => !x.Contains("internal"))
                .Select(File.ReadAllText)
                .Select(x => Regex.Match(x, @"public class (\w+) extends (\w+)"))
                .Where(x => x.Success && !x.Value.Contains("RaftException")) // Ignore duplicate RaftException.
                .Where(x => !x.Value.Contains("IgniteClient")) // Skip Java client exceptions.
                .ToDictionary(x => x.Groups[1].Value, x => x.Groups[2].Value);

            Assert.IsNotEmpty(javaExceptionsWithParents);

            var javaExceptions = javaExceptionsWithParents.Select(x => x.Key).Where(IsIgniteException).ToList();

            Assert.IsNotEmpty(javaExceptions);

            Assert.Multiple(() =>
            {
                var dotNetExceptions = typeof(IIgnite).Assembly.GetTypes()
                    .Select(t => t.Name)
                    .Where(x => x.EndsWith("Exception", StringComparison.Ordinal))
                    .ToImmutableHashSet();

                foreach (var exception in javaExceptions)
                {
                    Assert.IsTrue(dotNetExceptions.Contains(exception), "No .NET equivalent for Java exception: " + exception);
                }
            });

            bool IsIgniteException(string? ex) =>
                ex != null && (ex == "IgniteException" || IsIgniteException(javaExceptionsWithParents.GetValueOrDefault(ex)));
        }
    }
}
