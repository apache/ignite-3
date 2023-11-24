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
using System.Collections.Generic;
using System.Linq;
using Internal.Common;
using Microsoft.Extensions.Logging;

public class ListLoggerFactory : ILoggerFactory
{
    private readonly List<ListLogger.Entry> _entries = new();

    private readonly object _lock = new();

    public ListLoggerFactory(ICollection<LogLevel>? enabledLevels = null)
    {
        EnabledLevels = enabledLevels;
    }

    public ICollection<LogLevel>? EnabledLevels { get; }

    /// <summary>
    /// Gets the entries.
    /// </summary>
    public List<ListLogger.Entry> Entries
    {
        get
        {
            lock (_lock)
            {
                return _entries.ToList();
            }
        }
    }

    public void Dispose()
    {
        // No-op.
    }

    public ILogger CreateLogger(string categoryName) => new ListLogger(categoryName, AddEntry, enabledLevels: EnabledLevels);

    public void AddProvider(ILoggerProvider provider) => throw new NotSupportedException();

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

    private void AddEntry(ListLogger.Entry entry)
    {
        lock (_lock)
        {
            _entries.Add(entry);
        }
    }
}
