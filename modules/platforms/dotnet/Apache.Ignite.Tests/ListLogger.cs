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
    using Microsoft.Extensions.Logging;
    using NUnit.Framework;

    /// <summary>
    /// Stores log entries in a list.
    /// </summary>
    public class ListLogger : ILogger
    {
        private readonly string _categoryName;
        private readonly Action<Entry> _addEntry;

        public ListLogger(
            string categoryName,
            Action<Entry> addEntry,
            IEnumerable<LogLevel>? enabledLevels = null)
        {
            _categoryName = categoryName;
            _addEntry = addEntry;
            EnabledLevels = enabledLevels?.ToList() ?? new() { LogLevel.Debug, LogLevel.Information, LogLevel.Warning, LogLevel.Error };
        }

        /// <summary>
        /// Gets enabled levels.
        /// </summary>
        public List<LogLevel> EnabledLevels { get; }

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

            var message = formatter(state, exception);
            _addEntry(new Entry(message, logLevel, _categoryName, exception));
        }

        /** <inheritdoc /> */
        public bool IsEnabled(LogLevel logLevel) => EnabledLevels.Contains(logLevel);

        public IDisposable BeginScope<TState>(TState state)
            where TState : notnull
            => throw new NotImplementedException();

        [SuppressMessage("Microsoft.Design", "CA1034:NestedTypesShouldNotBeVisible", Justification = "Tests.")]
        public record Entry(string Message, LogLevel Level, string? Category, Exception? Exception);
    }
}
