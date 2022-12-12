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
    /// <para />
    /// Some of the logic here is similar to <see cref="ObjectSerializerHandler{T}"/>, but there are subtle differences:
    /// SQL has a different set of types;
    /// LINQ handles type conversion when possible;
    /// LINQ allows more ways to instantiate resulting objects.
    /// </summary>
    /// <param name="columns">Columns.</param>
    /// <param name="selectorExpression">Selector expression.</param>
    /// <param name="defaultIfNull">Whether to read null values as default for value types
    /// (when <see cref="Queryable.DefaultIfEmpty{TSource}(System.Linq.IQueryable{TSource})"/> is used).</param>
    /// <typeparam name="T">Result type.</typeparam>
    /// <returns>Row reader.</returns>
    public static RowReader<T> Get<T>(IReadOnlyList<IColumnMetadata> columns, Expression selectorExpression, bool defaultIfNull)
    {
        // Anonymous type projections use a constructor call. But user-defined types can also be used with constructor call.
        if (selectorExpression is NewExpression newExpr)
        {
            var ctorInfo = newExpr.Constructor!;
            var cacheKey = new ResultSelectorCacheKey<ConstructorInfo>(ctorInfo, columns, defaultIfNull);

            return (RowReader<T>)CtorCache.GetOrAdd(cacheKey, static k => EmitConstructorReader<T>(k.Target, k.Columns, k.DefaultIfNull));
        }

        if (columns.Count == 1 && typeof(T).ToSqlColumnType() is { } resType)
        {
            if (columns[0].Type == resType)
            {
                return static (IReadOnlyList<IColumnMetadata> cols, ref BinaryTupleReader reader) =>
                    (T)Sql.ReadColumnValue(ref reader, cols[0], 0)!;
            }

            return static (IReadOnlyList<IColumnMetadata> cols, ref BinaryTupleReader reader) =>
                (T)Convert.ChangeType(Sql.ReadColumnValue(ref reader, cols[0], 0)!, typeof(T), CultureInfo.InvariantCulture);
        }

        if (typeof(T).GetKeyValuePairTypes() is var (keyType, valType))
        {
            // TODO: Emit code.
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
            // TODO: Emit code.
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

    private static RowReader<T> EmitConstructorReader<T>(
        ConstructorInfo ctorInfo,
        IReadOnlyList<IColumnMetadata> columns,
        bool defaultAsNull)
    {
        var method = new DynamicMethod(
            name: "ConstructorFromBinaryTupleReader_" + typeof(T).FullName,
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
            Label endParamLabel = il.DefineLabel();

            var param = ctorParams[index];
            var col = columns[index];

            if (defaultAsNull)
            {
                // if (reader.IsNull(index)) return default;
                Label notNullLabel = il.DefineLabel();
                il.Emit(OpCodes.Ldarg_1); // Reader.
                il.Emit(OpCodes.Ldc_I4, index); // Index.
                il.Emit(OpCodes.Call, BinaryTupleMethods.IsNull);
                il.Emit(OpCodes.Brfalse_S, notNullLabel);

                if (param.ParameterType.IsValueType)
                {
                    var local = il.DeclareLocal(param.ParameterType);
                    il.Emit(OpCodes.Ldloca_S, local);
                    il.Emit(OpCodes.Initobj, param.ParameterType); // Load default value into local.
                    il.Emit(OpCodes.Ldloc, local); // Load local value onto stack for constructor call.
                }
                else
                {
                    il.Emit(OpCodes.Ldnull);
                }

                il.Emit(OpCodes.Br_S, endParamLabel);
                il.MarkLabel(notNullLabel);
            }

            il.Emit(OpCodes.Ldarg_1); // Reader.
            il.Emit(OpCodes.Ldc_I4, index); // Index.

            if (col.Type == SqlColumnType.Decimal)
            {
                il.Emit(OpCodes.Ldc_I4, col.Scale);
            }

            var colType = col.Type.ToClrType();
            il.Emit(OpCodes.Call, BinaryTupleMethods.GetReadMethod(colType));

            EmitConv(colType, param.ParameterType, il);
            il.MarkLabel(endParamLabel);
        }

        il.Emit(OpCodes.Newobj, ctorInfo);
        il.Emit(OpCodes.Ret);

        return (RowReader<T>)method.CreateDelegate(typeof(RowReader<T>));
    }

    private static RowReader<T> EmitUninitializedObjectReader<T>(
        IReadOnlyList<IColumnMetadata> columns,
        bool defaultAsNull)
    {
        var method = new DynamicMethod(
            name: "UninitializedObjectFromBinaryTupleReader_" + typeof(T).FullName,
            returnType: typeof(T),
            parameterTypes: new[] { typeof(IReadOnlyList<IColumnMetadata>), typeof(BinaryTupleReader).MakeByRefType() },
            m: typeof(IIgnite).Module,
            skipVisibility: true);

        var il = method.GetILGenerator();

        for (var index = 0; index < columns.Count; index++)
        {
            var col = columns[index];

            if (typeof(T).GetFieldByColumnName(col.Name) is not { } field)
            {
                continue;
            }

            Label endParamLabel = il.DefineLabel();

            if (defaultAsNull)
            {
                // if (reader.IsNull(index)) continue;
                il.Emit(OpCodes.Ldarg_1); // Reader.
                il.Emit(OpCodes.Ldc_I4, index); // Index.
                il.Emit(OpCodes.Call, BinaryTupleMethods.IsNull);
                il.Emit(OpCodes.Brtrue_S, endParamLabel);
            }

            il.Emit(OpCodes.Ldarg_1); // Reader.
            il.Emit(OpCodes.Ldc_I4, index); // Index.

            if (col.Type == SqlColumnType.Decimal)
            {
                il.Emit(OpCodes.Ldc_I4, col.Scale);
            }

            var colType = col.Type.ToClrType();
            il.Emit(OpCodes.Call, BinaryTupleMethods.GetReadMethod(colType));

            EmitConv(colType, param.ParameterType, il);
            il.MarkLabel(endParamLabel);
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
