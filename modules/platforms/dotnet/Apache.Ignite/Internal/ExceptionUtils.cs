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

namespace Apache.Ignite.Internal
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Reflection;

    /// <summary>
    /// Exception utils.
    /// </summary>
    internal static class ExceptionUtils
    {
        // TODO: Replace with source generator?
        private static readonly IReadOnlyDictionary<ReadOnlyMemory<char>, Type> ExceptionTypes = Assembly
            .GetExecutingAssembly()
            .GetTypes()
            .Where(t => typeof(IgniteException).IsAssignableFrom(t))
            .ToDictionary(x => x.Name.AsMemory(), x => x);

        /// <summary>
        /// Gets an Ignite exception.
        /// </summary>
        /// <param name="traceId">Trace id.</param>
        /// <param name="code">Code.</param>
        /// <param name="javaClass">Java class name.</param>
        /// <param name="message">Message.</param>
        /// <returns>Exception.</returns>
        public static IgniteException GetIgniteException(Guid traceId, int code, string javaClass, string? message)
        {
            var typeName = javaClass.AsMemory();
            var idx = typeName.Span.LastIndexOf('.');

            if (idx > 0 && idx < typeName.Length - 1)
            {
                typeName = typeName.Slice(idx + 1);
            }

            if (ExceptionTypes.TryGetValue(typeName, out var type))
            {
                return (IgniteException)Activator.CreateInstance(type, traceId, code, null);
            }

            return new IgniteException(traceId, code, message, new IgniteException(traceId, code, javaClass));
        }
    }
}
