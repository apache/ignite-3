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
    using Log;
    using NUnit.Framework;

    /// <summary>
    /// Stores log entries in a list.
    /// </summary>
    public class ListLogger : IIgniteLogger
    {
        /** */
        private readonly List<Entry> _entries = new();

        /** */
        private readonly object _lock = new();

        /** */
        private readonly IIgniteLogger? _wrappedLogger;

        public ListLogger(IIgniteLogger? wrappedLogger = null, IEnumerable<LogLevel>? enabledLevels = null)
        {
            _wrappedLogger = wrappedLogger;
            EnabledLevels = enabledLevels?.ToList() ?? new() { LogLevel.Debug, LogLevel.Info, LogLevel.Warn, LogLevel.Error };
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

        /** <inheritdoc /> */
        public void Log(
            LogLevel level,
            string message,
            object?[]? args,
            IFormatProvider? formatProvider,
            string? category,
            string? nativeErrorInfo,
            Exception? ex)
        {
            Assert.NotNull(message);

            if (!IsEnabled(level))
            {
                return;
            }

            _wrappedLogger?.Log(level, message, args, formatProvider, category, nativeErrorInfo, ex);

            lock (_lock)
            {
                if (args != null)
                {
                    message = string.Format(formatProvider, message, args);
                }

                if (ex != null)
                {
                    message += Environment.NewLine + ex;
                }

                _entries.Add(new Entry(message, level, category, ex));
            }
        }

        /** <inheritdoc /> */
        public bool IsEnabled(LogLevel level)
        {
            return EnabledLevels.Contains(level);
        }

        [SuppressMessage("Microsoft.Design", "CA1034:NestedTypesShouldNotBeVisible", Justification = "Tests.")]
        public record Entry(string Message, LogLevel Level, string? Category, Exception? Exception);
    }
}
