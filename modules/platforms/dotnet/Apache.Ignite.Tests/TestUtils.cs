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
    using System.Diagnostics;
    using System.IO;
    using System.Reflection;
    using System.Runtime.InteropServices;
    using System.Threading;
    using NUnit.Framework;

    public static class TestUtils
    {
        public static readonly string SolutionDir = GetSolutionDir();

        public static readonly string RepoRootDir = Path.Combine(GetSolutionDir(), "..", "..", "..");

        public static bool IsWindows => RuntimeInformation.IsOSPlatform(OSPlatform.Windows);

        public static void WaitForCondition(Func<bool> condition, int timeoutMs = 1000)
        {
            if (condition())
            {
                return;
            }

            var sw = Stopwatch.StartNew();

            while (sw.ElapsedMilliseconds < timeoutMs)
            {
                if (condition())
                {
                    return;
                }

                Thread.Sleep(50);
            }

            Assert.Fail("Condition not reached after " + sw.Elapsed);
        }

        private static string GetSolutionDir()
        {
            var dir = Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location);

            while (dir != null && !File.Exists(Path.Combine(dir, "Apache.Ignite.sln")))
            {
                dir = Path.GetDirectoryName(dir);
            }

            if (dir == null)
            {
                throw new Exception("Failed to locate solution directory.");
            }

            return dir;
        }
    }
}
