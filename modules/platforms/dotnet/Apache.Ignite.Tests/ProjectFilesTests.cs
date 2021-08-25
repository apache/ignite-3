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
    using System.IO;
    using NUnit.Framework;

    /// <summary>
    /// Checks project files.
    /// </summary>
    public class ProjectFilesTests
    {
        private static readonly IReadOnlyCollection<string> Types = new[]
        {
            "class", "struct", "record", "enum", "interface"
        };

        [Test]
        public void TestInternalNamespaceHasNoPublicTypes()
        {
            var files = Directory.GetFiles(TestUtils.SolutionDir, "*.cs", SearchOption.AllDirectories);

            var internalDir = Path.DirectorySeparatorChar + "Internal" + Path.DirectorySeparatorChar;

            foreach (var file in files)
            {
                if (!file.Contains(internalDir, StringComparison.OrdinalIgnoreCase))
                {
                    continue;
                }

                var text = File.ReadAllText(file);

                foreach (var type in Types)
                {
                    StringAssert.DoesNotContain("public " + type, text, file);
                }
            }
        }
    }
}
