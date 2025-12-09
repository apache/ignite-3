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
    using System.Diagnostics;
    using System.Globalization;
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
    public sealed class JavaServer : IDisposable
    {
        private const string GradleOptsEnvVar = "IGNITE_DOTNET_GRADLE_OPTS";
        private const string RequireExternalJavaServerEnvVar = "IGNITE_DOTNET_REQUIRE_EXTERNAL_SERVER";

        private const int ConnectTimeoutSeconds = 4 * 60;

        private const int MaxAttempts = 3;

        private const string GradleCommandExec = ":ignite-runner:runnerPlatformTest --parallel";

        private const string GradleCommandExecOldServer = ":ignite-compatibility-tests:runnerPlatformCompatibilityTest --parallel";

         /** Full path to Gradle binary. */
        private static readonly string GradlePath = GetGradle();

        private readonly Process? _process;

        private JavaServer(IReadOnlyList<int> ports, Process? process)
        {
            Port = ports[0];
            Ports = ports;
            _process = process;
        }

        public int Port { get; }

        public IReadOnlyList<int> Ports { get; }

        /// <summary>
        /// Starts a server node.
        /// </summary>
        /// <returns>Disposable object to stop the server.</returns>
        public static async Task<JavaServer> StartAsync() => await StartInternalAsyncWithRetry(
            async () => await StartInternalAsync(old: false, env: [], defaultPort: 10942));

        public static async Task<JavaServer> StartOldAsync(string version, string workDir) =>
            await StartInternalAsyncWithRetry(async () =>
            {
                // Get random unused ports to avoid conflicts with other tests.
                var ports = GetUnusedPorts(3);

                return await StartInternalAsync(
                    old: true,
                    env: new()
                    {
                        { "IGNITE_OLD_SERVER_VERSION", version },
                        { "IGNITE_OLD_SERVER_WORK_DIR", workDir },
                        { "IGNITE_OLD_SERVER_PORT", ports[0].ToString(CultureInfo.InvariantCulture) },
                        { "IGNITE_OLD_SERVER_HTTP_PORT", ports[1].ToString(CultureInfo.InvariantCulture) },
                        { "IGNITE_OLD_SERVER_CLIENT_PORT", ports[2].ToString(CultureInfo.InvariantCulture) },
                    });
            });

        public void Dispose()
        {
            if (_process == null)
            {
                Log(">>> Java server was not started by us, not stopping.");
                return;
            }

            Log(">>> Stopping Java server 1...");
            _process.StandardInput.Close();

            Log(">>> Stopping Java server 2...");
            _process.Kill();
            _process.Kill(entireProcessTree: true);

            Log(">>> Stopping Java server 3...");
            _process.Dispose();

            Log(">>> Stopping Java server 4...");
            KillProcessesOnPorts(Ports);

            Log(">>> Java server stopped.");
        }

        private static async Task<JavaServer> StartInternalAsyncWithRetry(
            Func<Task<JavaServer>> startFunc)
        {
            int attempt = 0;

            while (true)
            {
                try
                {
                    attempt++;
                    return await startFunc();
                }
                catch (Exception e)
                {
                    if (attempt == MaxAttempts)
                    {
                        Log($">>> Java server start failed after {MaxAttempts} attempts.");
                        throw;
                    }

                    Log($">>> Java server start attempt {attempt} failed: {e}. Retrying...");
                }
            }
        }

        private static async Task<JavaServer> StartInternalAsync(bool old, Dictionary<string, string?> env, int? defaultPort = null)
        {
            string gradleCommand = old ? GradleCommandExecOldServer : GradleCommandExec;

            if (defaultPort != null && await TryConnect(defaultPort.Value) == null)
            {
                // Server started from outside.
                Log(">>> Java server is already started on port " + defaultPort + ".");

                return new JavaServer([defaultPort.Value], null);
            }

            if (bool.TryParse(Environment.GetEnvironmentVariable(RequireExternalJavaServerEnvVar), out var requireExternalServer)
                && requireExternalServer)
            {
                throw new InvalidOperationException(
                    $"Java server is not started, but {RequireExternalJavaServerEnvVar} is set to true.");
            }

            Log(">>> Java server is not detected, starting...");

            var process = CreateProcess(gradleCommand, env);

            var tcs = new TaskCompletionSource<int[]>();

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
                    var ports = line.Split('=').Last().Split(',').Select(int.Parse).OrderBy(x => x).ToArray();
                    tcs.TrySetResult(ports);
                }

                if (line.StartsWith("Exception in thread \"main\"", StringComparison.OrdinalIgnoreCase) ||
                    line.Contains("Unable to start", StringComparison.OrdinalIgnoreCase))
                {
                    process.Kill(entireProcessTree: true);
                    tcs.TrySetException(new Exception($"Java server failed: {line}"));
                }
            };

            process.OutputDataReceived += handler;
            process.ErrorDataReceived += handler;

            process.Start();

            try
            {
                process.BeginOutputReadLine();
                process.BeginErrorReadLine();

                var ports = await tcs.Task.WaitAsync(TimeSpan.FromSeconds(ConnectTimeoutSeconds));
                var port = ports.FirstOrDefault();

                if (!WaitForServer(port))
                {
                    process.Kill(entireProcessTree: true);
                    KillProcessesOnPorts(ports);

                    throw new InvalidOperationException(
                        $"Failed to wait for the server to start (can't connect the client on port {port}). Check logs for details.");
                }

                Log($">>> Java server started on port {port}.");

                return new JavaServer(ports, process);
            }
            catch (Exception)
            {
                process.Kill(entireProcessTree: true);
                throw;
            }
        }

        private static Process CreateProcess(string gradleCommand, IDictionary<string, string?> env)
        {
            var file = TestUtils.IsWindows ? "cmd.exe" : "/bin/bash";
            var opts = Environment.GetEnvironmentVariable(GradleOptsEnvVar);
            var command = $"{GradlePath} {gradleCommand} {opts}";

            Log("Executing command: " + command);

            var processStartInfo = new ProcessStartInfo
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
                RedirectStandardError = true,
                RedirectStandardInput = true,
                Environment =
                {
                    ["IGNITE_COMPUTE_EXECUTOR_SERVER_SSL_SKIP_CERTIFICATE_VALIDATION"] = "true"
                }
            };

            foreach (var kvp in env)
            {
                processStartInfo.Environment[kvp.Key] = kvp.Value;
            }

            var process = new Process
            {
                StartInfo = processStartInfo
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

        private static void KillProcessesOnPorts(IEnumerable<int> ports)
        {
            foreach (var port in ports)
            {
                try
                {
                    KillProcessOnPort(port);
                }
                catch (Exception e)
                {
                    Log($"Failed to kill process on port {port}: {e}");
                }
            }
        }

        private static void KillProcessOnPort(int port)
        {
            var command = TestUtils.IsWindows
                ? $"for /f \"tokens=5\" %a in ('netstat -aon | find \"{port}\"') do taskkill /f /pid %a"
                : $"kill -9 $(lsof -t -i :{port})";

            var psi = new ProcessStartInfo
            {
                FileName = TestUtils.IsWindows ? "cmd.exe" : "/bin/sh",
                ArgumentList =
                {
                    TestUtils.IsWindows ? "/c" : "-c",
                    command
                },
                RedirectStandardOutput = false,
                RedirectStandardError = false,
                UseShellExecute = false,
                CreateNoWindow = true
            };

            using var process = Process.Start(psi);
            process?.WaitForExit();
        }

        private static int[] GetUnusedPorts(int count)
        {
            var ports = new int[count];
            var listeners = new List<TcpListener>();

            try
            {
                for (var i = 0; i < count; i++)
                {
                    var listener = new TcpListener(IPAddress.Loopback, 0);
                    listeners.Add(listener);

                    listener.Start();
                    ports[i] = ((IPEndPoint)listener.LocalEndpoint).Port;
                }

                return ports;
            }
            finally
            {
                foreach (var listener in listeners)
                {
                    listener.Stop();
                }
            }
        }
    }
}
