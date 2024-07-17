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
    using System.IO;
    using System.Linq;
    using Microsoft.CodeAnalysis;

    /// <summary>
    /// Source generator utils.
    /// </summary>
    internal static class GeneratorUtils
    {
        /// <summary>
        /// Gets the C# project root directory.
        /// </summary>
        /// <param name="context">Context.</param>
        /// <returns>Directory.</returns>
        public static string GetProjectRootDirectory(this GeneratorExecutionContext context) =>
            Path.GetDirectoryName(
                context.Compilation.SyntaxTrees.Single(x => x.FilePath.EndsWith("IIgnite.cs", StringComparison.Ordinal)).FilePath)!;

        /// <summary>
        /// Gets the modules directory.
        /// </summary>
        /// <param name="context">Context.</param>
        /// <returns>Modules directory.</returns>
        public static string GetJavaModulesDirectory(this GeneratorExecutionContext context) =>
            Path.GetFullPath(Path.Combine(context.GetProjectRootDirectory(), "..", "..", ".."));
    }
}
