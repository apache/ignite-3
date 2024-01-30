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
using System.Globalization;
using System.Text;
using Microsoft.Extensions.Logging;

/// <summary>
/// Console logger for tests. We don't use <see cref="Microsoft.Extensions.Logging.Console.ConsoleLogger"/> because it is asynchronous,
/// which means the log messages may not correspond to the current test.
/// </summary>
public class ConsoleLogger : ILogger, ILoggerFactory
{
    private readonly string _categoryName;
    private readonly LogLevel _minLevel;

    public ConsoleLogger(LogLevel minLevel)
        : this(string.Empty, minLevel)
    {
        // No-op.
    }

    public ConsoleLogger(string categoryName, LogLevel minLevel)
    {
        _categoryName = categoryName;
        _minLevel = minLevel;
    }

    public void Log<TState>(
        LogLevel logLevel,
        EventId eventId,
        TState state,
        Exception? exception,
        Func<TState, Exception?, string> formatter)
    {
        if (!IsEnabled(logLevel))
        {
            return;
        }

        var sb = new StringBuilder().AppendFormat(
            CultureInfo.InvariantCulture,
            "[{0:HH:mm:ss}] [{1}] [{2}] ",
            DateTime.Now,
            GetLogLevelString(logLevel),
            _categoryName);

        sb.Append(formatter(state, exception));

        if (exception != null)
        {
            sb.AppendFormat(CultureInfo.InvariantCulture, " (exception: {0})", exception);
        }

        Console.WriteLine(sb.ToString());
    }

    public bool IsEnabled(LogLevel logLevel) => logLevel >= _minLevel;

    public IDisposable BeginScope<TState>(TState state) => new DisposeAction(() => { });

    public void Dispose()
    {
        // No-op.
    }

    public ILogger CreateLogger(string categoryName) => new ConsoleLogger(categoryName, _minLevel);

    public void AddProvider(ILoggerProvider provider) => throw new NotSupportedException();

    private static string GetLogLevelString(LogLevel logLevel) =>
        logLevel switch
        {
            LogLevel.Trace => "trce",
            LogLevel.Debug => "dbug",
            LogLevel.Information => "info",
            LogLevel.Warning => "warn",
            LogLevel.Error => "fail",
            LogLevel.Critical => "crit",

            // ReSharper disable once PatternIsRedundant
            LogLevel.None or _ => throw new ArgumentOutOfRangeException(nameof(logLevel))
        };
}
