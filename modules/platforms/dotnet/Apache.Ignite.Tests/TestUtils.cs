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

namespace Apache.Ignite.Tests;

using System;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Common;
using Ignite.Transactions;
using Internal;
using Internal.Buffers;
using Internal.Common;
using Internal.Proto;
using Internal.Transactions;
using Microsoft.Extensions.Logging;

public static class TestUtils
{
    // Indicates long-running and/or memory/cpu intensive test.
    public const string CategoryIntensive = "LONG_TEST";

    public static readonly string SolutionDir = CommonTestUtils.SolutionDir;

    public static readonly string RepoRootDir = CommonTestUtils.RepoRootDir;

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

        throw new InvalidOperationException(message);
    }

    public static void WaitForCancellationRegistrations(CancellationTokenSource cts, int timeoutMs = 1000) =>
        WaitForCondition(
            condition: () => HasCallbacks(cts),
            timeoutMs: timeoutMs,
            messageFactory: () => "No callbacks registered in CancellationTokenSource");

    public static bool HasCallbacks(CancellationTokenSource cts)
    {
        var cb = cts
            .GetFieldValue<object?>("_registrations")?
            .GetFieldValue<object?>("Callbacks")?
            .GetFieldValue<object?>("Callback");

        return cb != null;
    }

    public static T GetFieldValue<T>(this object obj, string fieldName) => (T) GetNonPublicField(obj, fieldName).GetValue(obj)!;

    public static void SetFieldValue(this object obj, string fieldName, object? value) =>
        GetNonPublicField(obj, fieldName).SetValue(obj, value);

    public static bool IsRecordClass(this Type type) =>
        type.GetMethods().Any(m => m.Name == "<Clone>$" && m.ReturnType == type);

    public static ILoggerFactory GetConsoleLoggerFactory(LogLevel minLevel, bool autoFlush = true) =>
        new ConsoleLogger(minLevel)
        {
            AutoFlush = autoFlush
        };

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

    internal static async Task ForceLazyTxStart(ITransaction tx, IIgnite client, PreferredNode preferredNode = default) =>
        await LazyTransaction.EnsureStartedAsync(
            tx,
            ((IgniteClientInternal)client).Socket,
            preferredNode);

    private static FieldInfo GetNonPublicField(object obj, string fieldName)
    {
        var field = obj.GetType().GetField(fieldName, BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.Public);

        return field ?? throw new InvalidOperationException($"Field '{fieldName}' not found in type '{obj.GetType()}'.");
    }
}
