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
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Globalization;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.Serialization;
using Ignite.Sql;
using Proto.BinaryTuple;
using Remotion.Linq.Clauses;
using Remotion.Linq.Clauses.Expressions;
using Sql;
using Table.Serialization;

/// <summary>
/// Result selector cache.
/// </summary>
internal static class ResultSelector
{
    private static readonly ConcurrentDictionary<ConstructorInfo, object> CtorCache = new();

    /// <summary>
    /// Gets the result selector.
    /// </summary>
    /// <param name="columns">Columns.</param>
    /// <param name="selectorExpression">Selector expression.</param>
    /// <typeparam name="T">Result type.</typeparam>
    /// <returns>Row reader.</returns>
    public static RowReader<T> Get<T>(IReadOnlyList<IColumnMetadata> columns, Expression selectorExpression)
    {
        // TODO: IGNITE-18136 Replace reflection with emitted delegates.
        if (selectorExpression is NewExpression newExpr)
        {
            // Constructor projections always require the same set of columns, so the constructor itself can be the cache key.
            if (CtorCache.TryGetValue(newExpr.Constructor!, out var cachedCtor))
            {
                return (RowReader<T>) cachedCtor;
            }

            // TODO: Cache compiled delegate based on constructor? Do we care about column types here as well?
            return (IReadOnlyList<IColumnMetadata> cols, ref BinaryTupleReader reader) =>
            {
                var args = new object?[cols.Count];

                for (int i = 0; i < cols.Count; i++)
                {
                    var val = Sql.ReadColumnValue(ref reader, cols[i], i);

                    if (val != null)
                    {
                        val = Convert.ChangeType(val, newExpr.Arguments[i].Type, CultureInfo.InvariantCulture);
                    }

                    args[i] = val;
                }

                return (T)newExpr.Constructor!.Invoke(args);
            };
        }

        if (columns.Count == 1 && typeof(T).ToSqlColumnType() is not null)
        {
            return (IReadOnlyList<IColumnMetadata> cols, ref BinaryTupleReader reader) =>
                (T)Convert.ChangeType(Sql.ReadColumnValue(ref reader, cols[0], 0)!, typeof(T), CultureInfo.InvariantCulture);
        }

        if (typeof(T).GetKeyValuePairTypes() is var (keyType, valType))
        {
            return (IReadOnlyList<IColumnMetadata> cols, ref BinaryTupleReader reader) =>
            {
                var key = FormatterServices.GetUninitializedObject(keyType);
                var val = FormatterServices.GetUninitializedObject(valType);

                for (int i = 0; i < cols.Count; i++)
                {
                    var col = cols[i];
                    var colVal = Sql.ReadColumnValue(ref reader, col, i);

                    SetColumnValue(col, colVal, key, keyType);
                    SetColumnValue(col, colVal, val, valType);
                }

                return (T)Activator.CreateInstance(typeof(T), key, val)!;
            };
        }

        if (selectorExpression is QuerySourceReferenceExpression
            {
                ReferencedQuerySource: IFromClause { FromExpression: SubQueryExpression subQuery }
            })
        {
            // Select everything from a sub-query - use nested selector.
            return Get<T>(columns, subQuery.QueryModel.SelectClause.Selector);
        }

        return (IReadOnlyList<IColumnMetadata> cols, ref BinaryTupleReader reader) =>
        {
            var res = (T)FormatterServices.GetUninitializedObject(typeof(T));

            for (int i = 0; i < cols.Count; i++)
            {
                var col = cols[i];
                var val = Sql.ReadColumnValue(ref reader, col, i);

                SetColumnValue(col, val, res, typeof(T));
            }

            return res;
        };
    }

    private static void SetColumnValue<T>(IColumnMetadata col, object? val, T res, Type type)
    {
        if (type.GetFieldByColumnName(col.Name) is {} field)
        {
            if (val != null)
            {
                val = Convert.ChangeType(val, field.FieldType, CultureInfo.InvariantCulture);
            }

            field.SetValue(res, val);
        }
    }
}
