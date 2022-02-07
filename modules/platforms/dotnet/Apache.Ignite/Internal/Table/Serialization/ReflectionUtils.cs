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

namespace Apache.Ignite.Internal.Table.Serialization
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Linq;
    using System.Linq.Expressions;
    using System.Reflection;
    using System.Runtime.Serialization;

    /// <summary>
    /// Reflection utilities.
    /// </summary>
    internal static class ReflectionUtils
    {
        /// <summary>
        /// GetUninitializedObject method.
        /// </summary>
        public static readonly MethodInfo GetUninitializedObjectMethod = GetMethodInfo(
            () => FormatterServices.GetUninitializedObject(null!));

        /// <summary>
        /// GetTypeFromHandle method.
        /// </summary>
        public static readonly MethodInfo GetTypeFromHandleMethod = GetMethodInfo(() => Type.GetTypeFromHandle(default));

        private static readonly ConcurrentDictionary<Type, IReadOnlyDictionary<string, FieldInfo>> FieldsByNameCache = new();

        /// <summary>
        /// Gets all fields from the type, including non-public and inherited.
        /// </summary>
        /// <param name="type">Type.</param>
        /// <returns>Fields.</returns>
        public static IEnumerable<FieldInfo> GetAllFields(this Type type)
        {
            const BindingFlags flags = BindingFlags.Instance | BindingFlags.Public |
                                       BindingFlags.NonPublic | BindingFlags.DeclaredOnly;

            var t = type;

            while (t != null)
            {
                foreach (var field in t.GetFields(flags))
                {
                    yield return field;
                }

                t = t.BaseType;
            }
        }

        /// <summary>
        /// Gets the field by name ignoring case.
        /// </summary>
        /// <param name="type">Type.</param>
        /// <param name="name">Field name.</param>
        /// <returns>Field info, or null when no matching fields exist.</returns>
        public static FieldInfo? GetFieldIgnoreCase(this Type type, string name)
        {
            // ReSharper disable once HeapView.CanAvoidClosure, ConvertClosureToMethodGroup
            return FieldsByNameCache.GetOrAdd(type, t => GetFieldsByName(t)).TryGetValue(name, out var fieldInfo)
                ? fieldInfo
                : null;

            static IReadOnlyDictionary<string, FieldInfo> GetFieldsByName(Type type) =>
                type.GetAllFields().ToDictionary(f => f.GetCleanName(), StringComparer.OrdinalIgnoreCase);
        }

        /// <summary>
        /// Gets cleaned up member name without compiler-generated prefixes and suffixes.
        /// </summary>
        /// <param name="memberInfo">Member.</param>
        /// <returns>Clean name.</returns>
        public static string GetCleanName(this MemberInfo memberInfo) => CleanFieldName(memberInfo.Name);

        /// <summary>
        /// Cleans the field name and removes compiler-generated prefixes and suffixes.
        /// </summary>
        /// <param name="fieldName">Field name to clean.</param>
        /// <returns>Resulting field name.</returns>
        public static string CleanFieldName(string fieldName)
        {
            // C# auto property backing field (<MyProperty>k__BackingField)
            // or anonymous type backing field (<MyProperty>i__Field):
            if (fieldName.StartsWith("<", StringComparison.Ordinal)
                && fieldName.IndexOf(">", StringComparison.Ordinal) is var endIndex and > 0)
            {
                return fieldName.Substring(1, endIndex - 1);
            }

            // F# backing field:
            if (fieldName.EndsWith("@", StringComparison.Ordinal))
            {
                return fieldName.Substring(0, fieldName.Length - 1);
            }

            return fieldName;
        }

        /// <summary>
        /// Gets the method info.
        /// </summary>
        /// <param name="expression">Expression.</param>
        /// <typeparam name="T">Argument type.</typeparam>
        /// <returns>Corresponding MethodInfo.</returns>
        public static MethodInfo GetMethodInfo<T>(Expression<Func<T>> expression) => ((MethodCallExpression)expression.Body).Method;
    }
}
