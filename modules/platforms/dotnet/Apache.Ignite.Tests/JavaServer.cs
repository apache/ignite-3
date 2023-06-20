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
        private const string GradleOptsEnvVar = "IGNITE_DOTNET_GRADLE_OPTS";
        private const string RequireExternalJavaServerEnvVar = "IGNITE_DOTNET_REQUIRE_EXTERNAL_SERVER";

        private const int DefaultClientPort = 10942;

        private const int ConnectTimeoutSeconds = 120;

        private const string GradleCommandExec = ":ignite-runner:runnerPlatformTest"
          + " -x compileJava -x compileTestFixturesJava -x compileIntegrationTestJava -x compileTestJava --parallel";

         /** Full path to Gradle binary. */
        private static readonly string GradlePath = GetGradle();

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

            if (bool.TryParse(Environment.GetEnvironmentVariable(RequireExternalJavaServerEnvVar), out var requireExternalServer)
                && requireExternalServer)
            {
                throw new InvalidOperationException($"Java server is not started, but {RequireExternalJavaServerEnvVar} is set to true.");
            }

            Log(">>> Java server is not detected, starting...");

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
                    ports = line.Split('=').Last().Split(',').Select(int.Parse).OrderBy(x => x).ToArray();
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

        private static Process CreateProcess()
        {
            var file = TestUtils.IsWindows ? "cmd.exe" : "/bin/bash";
            var opts = Environment.GetEnvironmentVariable(GradleOptsEnvVar);
            var command = $"{GradlePath} {GradleCommandExec} {opts}";

            Log("Executing command: " + command);

            var process = new Process
            {
                StartInfo = new ProcessStartInfo
                {
                    FileName = file,
                    ArgumentList =
                    {
                        TestUtils.IsWindows ? "/c" : "-c",
                        command
                    },
                    CreateNoWindow = true,
                    UseShellExecute = false,
                    WorkingDirectory = TestUtils.RepoRootDir,
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
                var cfg = new IgniteClientConfiguration("127.0.0.1:" + port)
                {
                    SocketTimeout = TimeSpan.FromSeconds(0.5)
                };

                using var client = await IgniteClient.StartAsync(cfg);

                return null;
            }
            catch (Exception e)
            {
                Log(e.ToString());

                return e;
            }
        }

        private static string GetGradle()
        {
            var gradleWrapper = RuntimeInformation.IsOSPlatform(OSPlatform.Windows)
                ? "gradlew.bat"
                : "gradlew";

            return Path.Combine(TestUtils.RepoRootDir, gradleWrapper);
        }
    }
}
