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
using Microsoft.Extensions.Logging;

/// <summary>
/// Extension methods for <see cref="ILogger"/>.
/// </summary>
public static class LoggerExtensions
{
    public static ILoggerFactory ToLoggerFactory(this ILogger logger)
    {
        return new LoggerFactory(logger);
    }

    private class LoggerFactory : ILoggerFactory
    {
        private readonly ILogger _logger;

        public LoggerFactory(ILogger logger)
        {
            _logger = logger;
        }

        public void Dispose()
        {
            // No-op.
        }

        public ILogger CreateLogger(string categoryName) => _logger;

        public void AddProvider(ILoggerProvider provider) => throw new NotSupportedException();
    }
}
