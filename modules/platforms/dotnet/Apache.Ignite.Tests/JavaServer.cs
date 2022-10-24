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
    using System.Runtime.InteropServices;
    using System.Threading;
    using System.Threading.Tasks;
    using NUnit.Framework;

    /// <summary>
    /// Starts Java server nodes.
    /// </summary>
    public sealed class JavaServer : IDisposable
    {
        private const int DefaultClientPort = 10942;

        private const int ConnectTimeoutSeconds = 120;

        /** Maven command to execute the main class. */
        private const string MavenCommandExec = "exec:java@platform-test-node-runner";

        /** Maven arg to perform a dry run to ensure that code is compiled and all artifacts are downloaded. */
        private const string MavenCommandDryRunArg = " -Dexec.args=dry-run";

        /** Full path to Maven binary. */
        private static readonly string MavenPath = GetMaven();

        private static volatile bool _dryRunComplete;

        private readonly Process? _process;

        public JavaServer(int port, Process? process)
        {
            Port = port;
            _process = process;
        }

        public int Port { get; }

        /// <summary>
        /// Starts a server node.
        /// </summary>
        /// <returns>Disposable object to stop the server.</returns>
        public static async Task<JavaServer> StartAsync()
        {
            if (await TryConnect(DefaultClientPort) == null)
            {
                // Server started from outside.
                Log(">>> Java server is already started.");

                return new JavaServer(DefaultClientPort, null);
            }

            Log(">>> Java server is not detected, starting...");

            EnsureBuild();
            var process = CreateProcess();

            var evt = new ManualResetEventSlim(false);
            int[]? ports = null;

            DataReceivedEventHandler handler = (_, eventArgs) =>
            {
                var line = eventArgs.Data;
                if (line == null)
                {
                    return;
                }

                Log(line);

                if (line.StartsWith("THIN_CLIENT_PORTS", StringComparison.Ordinal))
                {
                    ports = line.Split('=').Last().Split(',').Select(int.Parse).ToArray();
                    evt.Set();
                }
            };

            process.OutputDataReceived += handler;
            process.ErrorDataReceived += handler;

            process.Start();

            process.BeginOutputReadLine();
            process.BeginErrorReadLine();

            var port = ports?.FirstOrDefault() ?? DefaultClientPort;

            if (!evt.Wait(TimeSpan.FromSeconds(ConnectTimeoutSeconds)) || !WaitForServer(port))
            {
                process.Kill(entireProcessTree: true);

                throw new InvalidOperationException("Failed to wait for the server to start. Check logs for details.");
            }

            Log($">>> Java server started on port {port}.");

            return new JavaServer(port, process);
        }

        public void Dispose()
        {
            _process?.Kill(entireProcessTree: true);
            _process?.Dispose();
            Log(">>> Java server stopped.");
        }

        /// <summary>
        /// Performs a dry run of the Maven executable to ensure that code is compiled and all artifacts are downloaded.
        /// Does not start the actual node.
        /// </summary>
        private static void EnsureBuild()
        {
            if (_dryRunComplete)
            {
                return;
            }

            using var process = CreateProcess(dryRun: true);

            DataReceivedEventHandler handler = (_, eventArgs) =>
            {
                var line = eventArgs.Data;
                if (line == null)
                {
                    return;
                }

                Log(line);
            };

            process.OutputDataReceived += handler;
            process.ErrorDataReceived += handler;

            process.Start();

            process.BeginErrorReadLine();
            process.BeginOutputReadLine();

            // 5 min timeout for the build process (may take time to download artifacts on slow networks).
            if (!process.WaitForExit(5 * 60_000))
            {
                process.Kill();
                throw new Exception("Failed to wait for Maven exec dry run.");
            }

            if (process.ExitCode != 0)
            {
                throw new Exception($"Maven exec failed with code {process.ExitCode}, check log for details.");
            }

            _dryRunComplete = true;
        }

        private static Process CreateProcess(bool dryRun = false)
        {
            var file = TestUtils.IsWindows ? "cmd.exe" : "/bin/bash";

            var process = new Process
            {
                StartInfo = new ProcessStartInfo
                {
                    FileName = file,
                    ArgumentList =
                    {
                        TestUtils.IsWindows ? "/c" : "-c",
                        $"{MavenPath} {MavenCommandExec}" + (dryRun ? MavenCommandDryRunArg : string.Empty)
                    },
                    CreateNoWindow = true,
                    UseShellExecute = false,
                    WorkingDirectory = Path.Combine(TestUtils.RepoRootDir, "modules", "runner"),
                    RedirectStandardOutput = true,
                    RedirectStandardError = true
                }
            };
            return process;
        }

        private static void Log(string? line)
        {
            // For IDE.
            Console.WriteLine(line);

            // For `dotnet test`.
            TestContext.Progress.WriteLine(line);
        }

        private static bool WaitForServer(int port)
        {
            var cts = new CancellationTokenSource();

            try
            {
                return TryConnectForever(port, cts.Token).Wait(TimeSpan.FromSeconds(ConnectTimeoutSeconds));
            }
            finally
            {
                cts.Cancel();
            }
        }

        private static async Task TryConnectForever(int port, CancellationToken ct)
        {
            while (await TryConnect(port) != null)
            {
                ct.ThrowIfCancellationRequested();
            }
        }

        private static async Task<Exception?> TryConnect(int port)
        {
            try
            {
                var cfg = new IgniteClientConfiguration("127.0.0.1:" + port);
                using var client = await IgniteClient.StartAsync(cfg);

                var tables = await client.Tables.GetTablesAsync();
                return tables.Count > 0 ? null : new InvalidOperationException("No tables found on server");
            }
            catch (Exception e)
            {
                Log(e.ToString());

                return e;
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
