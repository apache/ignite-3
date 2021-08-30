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
    using System.Linq;
    using System.Net;
    using System.Net.Sockets;
    using System.Runtime.InteropServices;
    using System.Threading;
    using System.Threading.Tasks;
    using NUnit.Framework;

    /// <summary>
    /// Starts Java server nodes.
    /// </summary>
    public static class JavaServer
    {
        public const int ClientPort = 10942;

        public static readonly IPEndPoint EndPoint = new(IPAddress.Loopback, ClientPort);

        /** Maven command to execute the main class. */
        private const string MavenCommandExec =
            "-Dtest=ITThinClientConnectionTest -DfailIfNoTests=false -DIGNITE_TEST_KEEP_NODES_RUNNING=true " +
            "surefire:test";

        /** Full path to Maven binary. */
        private static readonly string MavenPath = GetMaven();

        /// <summary>
        /// Starts a server node.
        /// </summary>
        /// <returns>Disposable object to stop the server.</returns>
        public static async Task<IDisposable?> Start()
        {
            if (await TryConnect())
            {
                // Server started from outside.
                return null;
            }

            var file = TestUtils.IsWindows ? "cmd.exe" : "/bin/bash";

            var process = new Process
            {
                StartInfo = new ProcessStartInfo
                {
                    FileName = file,
                    ArgumentList =
                    {
                        TestUtils.IsWindows ? "/c" : "-c",
                        $"{MavenPath} {MavenCommandExec}"
                    },
                    CreateNoWindow = true,
                    UseShellExecute = false,
                    WorkingDirectory = TestUtils.RepoRootDir,
                    RedirectStandardOutput = true,
                    RedirectStandardError = true
                }
            };

            DataReceivedEventHandler handler = (_, eventArgs) =>
            {
                if (eventArgs.Data == null)
                {
                    return;
                }

                // For IDE
                Console.WriteLine(eventArgs.Data);

                // For `dotnet test`
                TestContext.Progress.WriteLine(eventArgs.Data);
            };

            process.OutputDataReceived += handler;
            process.ErrorDataReceived += handler;

            process.Start();

            process.BeginOutputReadLine();
            process.BeginErrorReadLine();

            if (!WaitForServer())
            {
                process.Kill(true);

                throw new InvalidOperationException("Failed to wait for the server to start.");
            }

            return new DisposeAction(() => process.Kill(true));
        }

        private static bool WaitForServer()
        {
            var cts = new CancellationTokenSource();

            try
            {
                return TryConnectForever(cts.Token).Wait(TimeSpan.FromSeconds(15));
            }
            finally
            {
                cts.Cancel();
            }
        }

        private static async Task TryConnectForever(CancellationToken ct)
        {
            while (!await TryConnect())
            {
                ct.ThrowIfCancellationRequested();
            }
        }

        private static async Task<bool> TryConnect()
        {
            try
            {
                using Socket socket = new(SocketType.Stream, ProtocolType.Tcp)
                {
                    NoDelay = true
                };

                await socket.ConnectAsync(IPAddress.Loopback, ClientPort);

                return true;
            }
            catch
            {
                return false;
            }
        }

        /// <summary>
        /// Gets maven path.
        /// </summary>
        private static string GetMaven()
        {
            var extensions = RuntimeInformation.IsOSPlatform(OSPlatform.Windows)
                ? new[] {".cmd", ".bat"}
                : new[] {string.Empty};

            return new[] {"MAVEN_HOME", "M2_HOME", "M3_HOME", "MVN_HOME"}
                .Select(Environment.GetEnvironmentVariable)
                .Where(x => !string.IsNullOrEmpty(x))
                .Select(x => Path.Combine(x!, "bin", "mvn"))
                .SelectMany(x => extensions.Select(ext => x + ext))
                .Where(File.Exists)
                .FirstOrDefault() ?? "mvn";
        }
    }
}
