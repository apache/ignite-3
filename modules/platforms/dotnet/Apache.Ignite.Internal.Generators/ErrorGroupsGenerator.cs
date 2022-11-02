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
    using System.Collections.Generic;
    using System.Globalization;
    using System.IO;
    using System.Linq;
    using System.Reflection;
    using System.Text;
    using System.Text.RegularExpressions;
    using Microsoft.CodeAnalysis;

    /// <summary>
    /// Generates error groups source from ErrorGroups.java.
    /// </summary>
    [Generator]
    public sealed class ErrorGroupsGenerator : JavaToCsharpGeneratorBase
    {
        /// <inheritdoc/>
        protected override IEnumerable<(string Name, string Code)> ExecuteInternal(GeneratorExecutionContext context)
        {
            var javaErrorGroupsFile = Path.GetFullPath(Path.Combine(
                context.GetJavaModulesDirectory(),
                "core",
                "src",
                "main",
                "java",
                "org",
                "apache",
                "ignite",
                "lang",
                "ErrorGroups.java"));

            if (!File.Exists(javaErrorGroupsFile))
            {
                throw new Exception("File not found: " + javaErrorGroupsFile + " = " + Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location)!);
            }

            var javaErrorGroupsText = File.ReadAllText(javaErrorGroupsFile);

            // ErrorGroup TX_ERR_GROUP = ErrorGroup.newGroup("TX", 7);
            var javaErrorGroups = Regex.Matches(
                    javaErrorGroupsText,
                    @"public static class ([A-Za-z]+) {\s+/\*\*.*?\*/\s+public static final ErrorGroup ([\w_]+)_ERR_GROUP = ErrorGroup.newGroup\(""([A-Z]+)"", (\d+)",
                    RegexOptions.Singleline | RegexOptions.CultureInvariant)
                .Cast<Match>()
                .Select(x => (ClassName: x.Groups[1].Value, GroupName: x.Groups[2].Value, ShortGroupName: x.Groups[3].Value, Code: int.Parse(x.Groups[4].Value, CultureInfo.InvariantCulture)))
                .ToList();

            if (javaErrorGroups.Count == 0)
            {
                throw new Exception($"Failed to parse Java error groups from '{javaErrorGroupsFile}'");
            }

            var sb = new StringBuilder();

            sb.AppendLine("// <auto-generated/>");
            sb.AppendLine("namespace Apache.Ignite");
            sb.AppendLine("{");
            sb.AppendLine("    using System;\n");
            sb.AppendLine("    public static partial class ErrorGroups");
            sb.AppendLine("    {");

            // GetGroupName.
            sb.AppendLine(
@"        /// <summary>
        /// Gets the group name by code.
        /// </summary>
        /// <param name=""groupCode"">Group code.</param>
        /// <returns>Group name.</returns>
        public static string GetGroupName(int groupCode) => groupCode switch
        {");

            foreach (var (className, _, _, _) in javaErrorGroups)
            {
                sb.AppendLine(@$"            {className}.GroupCode => {className}.GroupName,");
            }

            sb.AppendLine(
                @"
            _ => UnknownGroupName
        };");

            // Groups.
            foreach (var (className, groupName, shortGroupName, groupCode) in javaErrorGroups)
            {
                sb.AppendLine();
                sb.AppendLine($"        /// <summary> {className} errors. </summary>");
                sb.AppendLine($"        public static class {className}");
                sb.AppendLine($"        {{");
                sb.AppendLine($"            /// <summary> {className} group code. </summary>");
                sb.AppendLine($"            public const int GroupCode = {groupCode};");
                sb.AppendLine();
                sb.AppendLine($"            /// <summary> {className} group name. </summary>");
                sb.AppendLine($"            public const string GroupName = \"{shortGroupName}\";");

                // TX_STATE_STORAGE_CREATE_ERR = TX_ERR_GROUP.registerErrorCode(1)
                var javaErrors = Regex.Matches(
                        javaErrorGroupsText,
                        @"([\w_]+)_ERR = " + groupName + @"_ERR_GROUP\.registerErrorCode\((\d+)\);")
                    .Cast<Match>()
                    .Select(x => (Name: x.Groups[1].Value, Code: int.Parse(x.Groups[2].Value, CultureInfo.InvariantCulture)))
                    .ToList();

                if (javaErrors.Count == 0)
                {
                    throw new Exception($"Failed to parse Java errors for group {groupName} from '{javaErrorGroupsFile}'");
                }

                foreach (var (errorName, errorCode) in javaErrors)
                {
                    var dotNetErrorName = SnakeToCamelCase(errorName);

                    sb.AppendLine();
                    sb.AppendLine($"            /// <summary> {dotNetErrorName} error. </summary>");
                    sb.AppendLine($"            public const int {dotNetErrorName} = (GroupCode << 16) | ({errorCode} & 0xFFFF);");
                }

                sb.AppendLine("        }");
            }

            sb.AppendLine("    }");
            sb.AppendLine("}");

            yield return ("ErrorGroups.g.cs", sb.ToString());
        }

        private static string SnakeToCamelCase(string str) =>
            string.Concat(str.Split('_').Select(x => x.Substring(0, 1).ToUpperInvariant() + x.Substring(1).ToLowerInvariant()));
    }
}
