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
    using System.ComponentModel.DataAnnotations.Schema;
    using System.Diagnostics.CodeAnalysis;
    using System.Linq.Expressions;
    using System.Reflection;
    using System.Runtime.CompilerServices;

    /// <summary>
    /// Reflection utilities.
    /// </summary>
    [RequiresUnreferencedCode(TrimWarning)]
    internal static class ReflectionUtils
    {
        /// <summary>
        /// GetUninitializedObject method.
        /// </summary>
        public static readonly MethodInfo GetUninitializedObjectMethod = GetMethodInfo(
            () => RuntimeHelpers.GetUninitializedObject(null!));

        /// <summary>
        /// GetTypeFromHandle method.
        /// </summary>
        public static readonly MethodInfo GetTypeFromHandleMethod = GetMethodInfo(() => Type.GetTypeFromHandle(default));

        /// <summary>
        /// AOT and trimming warning.
        /// </summary>
        internal const string TrimWarning =
            "Object mapping requires reflection and IL emit and does not support AOT and trimming scenarios. " +
            "Use overloads with IMapper<T> instead.";

        private static readonly ConcurrentDictionary<Type, IReadOnlyDictionary<string, ColumnInfo>> FieldsByColumnNameCache = new();

        /// <summary>
        /// Gets column names for all fields in the specified type.
        /// </summary>
        /// <param name="type">Type.</param>
        /// <returns>Columns.</returns>
        public static ICollection<ColumnInfo> GetColumns(this Type type) => (ICollection<ColumnInfo>)GetFieldsByColumnName(type).Values;

        /// <summary>
        /// Gets a pair of types for <see cref="KeyValuePair{TKey,TValue}"/>.
        /// </summary>
        /// <param name="type">Type.</param>
        /// <returns>Resulting pair, or null when specified type is not <see cref="KeyValuePair{TKey,TValue}"/>.</returns>
        public static (Type KeyType, Type ValType)? GetKeyValuePairTypes(this Type type)
        {
            if (!type.IsKeyValuePair())
            {
                return null;
            }

            var types = type.GetGenericArguments();

            return (types[0], types[1]);
        }

        /// <summary>
        /// Gets a value indicating whether the type is <see cref="KeyValuePair{TKey,TValue}"/>.
        /// </summary>
        /// <param name="type">Type.</param>
        /// <returns>Whether the provided type is a <see cref="KeyValuePair{TKey,TValue}"/>.</returns>
        public static bool IsKeyValuePair(this Type? type) =>
            type is { IsGenericType: true } && type.GetGenericTypeDefinition() == typeof(KeyValuePair<,>);

        /// <summary>
        /// Gets the underlying enum type, if applicable. Otherwise, returns the type itself.
        /// </summary>
        /// <param name="type">Type to unwrap.</param>
        /// <returns>Underlying type when enum; type itself otherwise.</returns>
        public static Type UnwrapEnum(this Type type)
        {
            if (Nullable.GetUnderlyingType(type) is { IsEnum: true } underlyingType)
            {
                return typeof(Nullable<>).MakeGenericType(Enum.GetUnderlyingType(underlyingType));
            }

            return type.IsEnum
                ? Enum.GetUnderlyingType(type)
                : type;
        }

        /// <summary>
        /// Gets a map of fields by column name. Ignores case, handles <see cref="ColumnAttribute"/> and <see cref="NotMappedAttribute"/>.
        /// </summary>
        /// <param name="type">Type to get the map for.</param>
        /// <returns>Map.</returns>
        public static IReadOnlyDictionary<string, ColumnInfo> GetFieldsByColumnName(this Type type)
        {
            // ReSharper disable once HeapView.CanAvoidClosure, HeapView.ClosureAllocation, HeapView.DelegateAllocation (false positive)
            return FieldsByColumnNameCache.GetOrAdd(type, static t => RetrieveFieldsByColumnName(t));

            static IReadOnlyDictionary<string, ColumnInfo> RetrieveFieldsByColumnName(Type type)
            {
                var res = new Dictionary<string, ColumnInfo>(StringComparer.OrdinalIgnoreCase);

                foreach (var field in type.GetAllFields())
                {
                    var columnInfo = field.GetColumnInfo();

                    if (columnInfo == null)
                    {
                        continue;
                    }

                    if (res.TryGetValue(columnInfo.Name, out var existingColInfo))
                    {
                        throw new ArgumentException(
                            $"Column '{columnInfo.Name}' maps to more than one field of type {type}: {field} and {existingColInfo.Field}");
                    }

                    res.Add(columnInfo.Name, columnInfo);
                }

                return res;
            }
        }

        /// <summary>
        /// Gets all fields from the type, including non-public and inherited.
        /// </summary>
        /// <param name="type">Type.</param>
        /// <returns>Fields.</returns>
        private static IEnumerable<FieldInfo> GetAllFields(this Type type)
        {
            if (type.IsPrimitive)
            {
                yield break;
            }

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
        /// Gets cleaned up member name without compiler-generated prefixes and suffixes.
        /// </summary>
        /// <param name="memberInfo">Member.</param>
        /// <returns>Clean name.</returns>
        private static string GetCleanName(this MemberInfo memberInfo) => CleanFieldName(memberInfo.Name);

        /// <summary>
        /// Gets column name for the specified field: uses <see cref="ColumnAttribute"/> when available,
        /// falls back to cleaned up field name otherwise.
        /// </summary>
        /// <param name="fieldInfo">Member.</param>
        /// <returns>Clean name.</returns>
        private static ColumnInfo? GetColumnInfo(this FieldInfo fieldInfo)
        {
            if (fieldInfo.GetCustomAttribute<NotMappedAttribute>() != null)
            {
                return null;
            }

            if (fieldInfo.GetCustomAttribute<ColumnAttribute>() is { Name: { } columnAttributeName })
            {
                return new(columnAttributeName, fieldInfo, null, HasColumnNameAttribute: true);
            }

            var cleanName = fieldInfo.GetCleanName();

            if (fieldInfo.IsDefined(typeof(CompilerGeneratedAttribute), inherit: true) &&
                fieldInfo.DeclaringType?.GetProperty(cleanName) is { } property)
            {
                if (property.GetCustomAttribute<NotMappedAttribute>() != null)
                {
                    return null;
                }

                if (property.GetCustomAttribute<ColumnAttribute>() is { Name: { } columnAttributeName2 })
                {
                    // This is a compiler-generated backing field for an automatic property - get the attribute from the property.
                    return new(columnAttributeName2, fieldInfo, property, HasColumnNameAttribute: true);
                }

                return new(cleanName, fieldInfo, property, HasColumnNameAttribute: false);
            }

            return new(cleanName, fieldInfo, null, HasColumnNameAttribute: false);
        }

        /// <summary>
        /// Cleans the field name and removes compiler-generated prefixes and suffixes.
        /// </summary>
        /// <param name="fieldName">Field name to clean.</param>
        /// <returns>Resulting field name.</returns>
        private static string CleanFieldName(string fieldName)
        {
            // C# auto property backing field (<MyProperty>k__BackingField)
            // or anonymous type backing field (<MyProperty>i__Field):
            if (fieldName.StartsWith('<')
                && fieldName.IndexOf('>', StringComparison.Ordinal) is var endIndex and > 0)
            {
                return fieldName.Substring(1, endIndex - 1);
            }

            // F# backing field:
            if (fieldName.EndsWith('@'))
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
        private static MethodInfo GetMethodInfo<T>(Expression<Func<T>> expression) => ((MethodCallExpression)expression.Body).Method;

        /// <summary>
        /// Column info.
        /// </summary>
        /// <param name="Name">Column name.</param>
        /// <param name="Field">Corresponding field.</param>
        /// <param name="Property">Corresponding property (when <see cref="Field"/> is a backing field of a auto property).</param>
        /// <param name="HasColumnNameAttribute">Whether corresponding field or property has <see cref="ColumnAttribute"/>
        /// with <see cref="ColumnAttribute.Name"/> set.</param>
        internal record ColumnInfo(string Name, FieldInfo Field, PropertyInfo? Property, bool HasColumnNameAttribute);
    }
}
