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
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Reflection.Emit;
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
    private static readonly ConcurrentDictionary<ResultSelectorCacheKey<ConstructorInfo>, object> CtorCache = new();

    /// <summary>
    /// Gets the result selector.
    /// </summary>
    /// <param name="columns">Columns.</param>
    /// <param name="selectorExpression">Selector expression.</param>
    /// <param name="defaultIfNull">Whether to read null values as default for value types
    /// (when <see cref="Queryable.DefaultIfEmpty{TSource}(System.Linq.IQueryable{TSource})"/> is used).</param>
    /// <typeparam name="T">Result type.</typeparam>
    /// <returns>Row reader.</returns>
    public static RowReader<T> Get<T>(IReadOnlyList<IColumnMetadata> columns, Expression selectorExpression, bool defaultIfNull)
    {
        // TODO: IGNITE-18136 Replace reflection with emitted delegates.
        // Anonymous type projections use a constructor call. But user-defined types can also be used with constructor call.
        if (selectorExpression is NewExpression newExpr)
        {
            var ctorInfo = newExpr.Constructor!;
            var cacheKey = new ResultSelectorCacheKey<ConstructorInfo>(ctorInfo, columns);

            return (RowReader<T>)CtorCache.GetOrAdd(cacheKey, static k => EmitConstructorReader<T>(k.Target, k.Columns));
        }

        if (columns.Count == 1 && typeof(T).ToSqlColumnType() is not null)
        {
            // TODO: No need for emit, but cache the logic based on column type and result type to avoid conversions and switch.
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
            return Get<T>(columns, subQuery.QueryModel.SelectClause.Selector, defaultIfNull);
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

    private static RowReader<T> EmitConstructorReader<T>(ConstructorInfo ctorInfo, IReadOnlyList<IColumnMetadata> columns)
    {
        var method = new DynamicMethod(
            name: "ConstructFromBinaryTupleReader_" + ctorInfo.DeclaringType!.Name,
            returnType: typeof(T),
            parameterTypes: new[] { typeof(IReadOnlyList<IColumnMetadata>), typeof(BinaryTupleReader).MakeByRefType() },
            m: typeof(IIgnite).Module,
            skipVisibility: true);

        var il = method.GetILGenerator();
        var ctorParams = ctorInfo.GetParameters();

        if (ctorParams.Length != columns.Count)
        {
            throw new InvalidOperationException("Constructor parameter count does not match column count, can't emit row reader.");
        }

        for (var index = 0; index < ctorParams.Length; index++)
        {
            // TODO IGNITE-18329 handle nulls.
            // But this screws up outer joins.
            var param = ctorParams[index];
            var col = columns[index];

            il.Emit(OpCodes.Ldarg_1); // Reader.
            il.Emit(OpCodes.Ldc_I4, index); // Index.

            if (col.Type == SqlColumnType.Decimal)
            {
                // TODO: Test for this.
                il.Emit(OpCodes.Ldc_I4, col.Scale);
            }

            var colType = col.Type.ToClrType();
            il.Emit(OpCodes.Call, BinaryTupleMethods.GetReadMethod(colType));

            EmitConv(colType, param.ParameterType, il);
        }

        il.Emit(OpCodes.Newobj, ctorInfo);
        il.Emit(OpCodes.Ret);

        return (RowReader<T>)method.CreateDelegate(typeof(RowReader<T>));
    }

    private static void EmitConv(Type from, Type to, ILGenerator il)
    {
        if (from == to)
        {
            return;
        }

        // TODO: Support all types and test them.
        // TODO: Use a dictionary of opcodes?
        if (to == typeof(int))
        {
            il.Emit(OpCodes.Conv_I4);
        }
        else if (to == typeof(double))
        {
            il.Emit(OpCodes.Conv_R8);
        }
    }
}
