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

namespace Apache.Ignite.Internal.Linq;

using System;
using System.Collections.Generic;

/// <summary>
/// SQL type mapping.
/// </summary>
internal static class SqlTypes // TODO: Replace with SqlColumnTypeExtensions.
{
    /** */
    private static readonly Dictionary<Type, string> NetToSql = new()
    {
        {typeof(bool), "boolean"},
        {typeof(sbyte), "tinyint"},
        {typeof(short), "smallint"},
        {typeof(int), "int"},
        {typeof(long), "bigint"},
        {typeof(float), "real"},
        {typeof(double), "double"},
        {typeof(string), "nvarchar"},
        {typeof(decimal), "decimal"},
        {typeof(Guid), "uuid"},
        {typeof(DateTime), "timestamp"},
    };

    /** */
    private static readonly HashSet<Type> NotSupportedTypes = new()
    {
        typeof(char),
        typeof(byte),
        typeof(ushort),
        typeof(uint),
        typeof(ulong)
    };

    /// <summary>
    /// Gets the corresponding Java type name.
    /// </summary>
    /// <param name="type">CLR type.</param>
    /// <returns>SQL type name.</returns>
    public static string? GetSqlTypeName(Type? type)
    {
        if (type == null)
        {
            return null;
        }

        type = Nullable.GetUnderlyingType(type) ?? type;

        if (NotSupportedTypes.Contains(type))
        {
            throw new NotSupportedException("Type is not supported for SQL mapping: " + type);
        }

        return NetToSql.TryGetValue(type, out var res) ? res : null;
    }
}
