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
    using System.Reflection;
    using System.Runtime.InteropServices;
    using System.Threading.Tasks;
    using Ignite.Transactions;
    using Internal;
    using Internal.Buffers;
    using Internal.Common;
    using Internal.Proto;
    using Internal.Transactions;
    using Microsoft.Extensions.Logging;
    using NodaTime;
    using NUnit.Framework;

    public static class TestUtils
    {
        public static readonly string SolutionDir = GetSolutionDir();

        public static readonly string RepoRootDir = Path.Combine(GetSolutionDir(), "..", "..", "..");

        public static bool IsWindows => RuntimeInformation.IsOSPlatform(OSPlatform.Windows);

        public static Func<LocalTime, LocalTime> TruncateTimeToMillis { get; } =
            time => new LocalTime(time.Hour, time.Minute, time.Second, time.Millisecond);

        public static void WaitForCondition(Func<bool> condition, int timeoutMs = 1000, Func<string>? messageFactory = null) =>
            WaitForConditionAsync(() => Task.FromResult(condition()), timeoutMs, messageFactory).GetAwaiter().GetResult();

        public static async Task WaitForConditionAsync(
            Func<Task<bool>> condition,
            int timeoutMs = 1000,
            Func<string>? messageFactory = null)
        {
            if (await condition())
            {
                return;
            }

            var sw = Stopwatch.StartNew();

            while (sw.ElapsedMilliseconds < timeoutMs)
            {
                if (await condition())
                {
                    return;
                }

                await Task.Delay(10);
            }

            var message = "Condition not reached after " + sw.Elapsed;

            if (messageFactory != null)
            {
                message += $" ({messageFactory()})";
            }

            Assert.Fail(message);
        }

        public static T GetFieldValue<T>(this object obj, string fieldName) => (T) GetNonPublicField(obj, fieldName).GetValue(obj)!;

        public static void SetFieldValue(this object obj, string fieldName, object? value) =>
            GetNonPublicField(obj, fieldName).SetValue(obj, value);

        public static ILoggerFactory GetConsoleLoggerFactory(LogLevel minLevel) => new ConsoleLogger(minLevel);

        public static void CheckByteArrayPoolLeak(int timeoutMs = 1000)
        {
#if DEBUG
            WaitForCondition(
                condition: () => ByteArrayPool.CurrentlyRentedArrays.IsEmpty,
                timeoutMs: timeoutMs,
                messageFactory: () =>
                {
                    var bufs = ByteArrayPool.CurrentlyRentedArrays
                        .Select(x => $"{x.Value.DeclaringType}.{x.Value.Name}")
                        .StringJoin();

                    return $"Leaked buffers: {bufs}";
                });
#endif
        }

        public static Instant TruncateInstantToMillis(Instant instant)
        {
            return Instant.FromUnixTimeMilliseconds(instant.ToUnixTimeMilliseconds());
        }

        internal static async Task ForceLazyTxStart(ITransaction tx, IIgnite client, PreferredNode preferredNode = default) =>
            await LazyTransaction.EnsureStartedAsync(
                tx,
                client.GetFieldValue<ClientFailoverSocket>("_socket"),
                preferredNode);

        private static FieldInfo GetNonPublicField(object obj, string fieldName)
        {
            var field = obj.GetType().GetField(fieldName, BindingFlags.Instance | BindingFlags.NonPublic);
            Assert.IsNotNull(field, $"Field '{fieldName}' not found in '{obj.GetType()}'");

            return field!;
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
