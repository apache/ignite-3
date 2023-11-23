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
    using System.Diagnostics.CodeAnalysis;
    using System.Linq;
    using Internal.Common;
    using Microsoft.Extensions.Logging;
    using NUnit.Framework;

    /// <summary>
    /// Stores log entries in a list.
    /// </summary>
    public class ListLogger : ILogger
    {
        /** */
        private readonly List<Entry> _entries = new();

        /** */
        private readonly object _lock = new();

        /** */
        private readonly ILogger? _wrappedLogger;

        public ListLogger(ILogger? wrappedLogger = null, IEnumerable<LogLevel>? enabledLevels = null)
        {
            _wrappedLogger = wrappedLogger;
            EnabledLevels = enabledLevels?.ToList() ?? new() { LogLevel.Debug, LogLevel.Information, LogLevel.Warning, LogLevel.Error };
        }

        /// <summary>
        /// Gets enabled levels.
        /// </summary>
        public List<LogLevel> EnabledLevels { get; }

        /// <summary>
        /// Gets the entries.
        /// </summary>
        public List<Entry> Entries
        {
            get
            {
                lock (_lock)
                {
                    return _entries.ToList();
                }
            }
        }

        /// <summary>
        /// Gets the log as a string.
        /// </summary>
        /// <returns>Log string.</returns>
        public string GetLogString()
        {
            lock (_lock)
            {
                return _entries.Select(e => $"{e.Category} [{e.Level}] {e.Message}").StringJoin();
            }
        }

        /// <summary>
        /// Clears the entries.
        /// </summary>
        public void Clear()
        {
            lock (_lock)
            {
                _entries.Clear();
            }
        }

        public void Log<TState>(
            LogLevel logLevel,
            EventId eventId,
            TState state,
            Exception? exception,
            Func<TState, Exception?, string> formatter)
        {
            Assert.NotNull(state);
            Assert.NotNull(formatter);

            if (!IsEnabled(logLevel))
            {
                return;
            }

            _wrappedLogger?.Log(logLevel, eventId, state, exception, formatter);

            lock (_lock)
            {
                var message = formatter(state, exception);

                // TODO: Category?
                _entries.Add(new Entry(message, logLevel, null, exception));
            }
        }

        /** <inheritdoc /> */
        public bool IsEnabled(LogLevel logLevel)
        {
            return EnabledLevels.Contains(logLevel);
        }

        public IDisposable BeginScope<TState>(TState state)
        {
            throw new NotImplementedException();
        }

        [SuppressMessage("Microsoft.Design", "CA1034:NestedTypesShouldNotBeVisible", Justification = "Tests.")]
        public record Entry(string Message, LogLevel Level, string? Category, Exception? Exception);
    }
}
