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
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Reflection.Emit;
using System.Threading;
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

    private static readonly ConcurrentDictionary<ResultSelectorCacheKey<Type>, object> SingleColumnReaderCache = new();

    private static readonly ConcurrentDictionary<ResultSelectorCacheKey<Type>, object> ReaderCache = new();

    private static readonly ConcurrentDictionary<ResultSelectorCacheKey<(Type Key, Type Val)>, object> KvReaderCache = new();

    private static long _idCounter;

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
            var ctorCacheKey = new ResultSelectorCacheKey<ConstructorInfo>(ctorInfo, columns, defaultIfNull);

            return (RowReader<T>)CtorCache.GetOrAdd(
                ctorCacheKey,
                static k => EmitConstructorReader<T>(k.Target, k.Columns, k.DefaultIfNull));
        }

        // TODO IGNITE-18435 Full enum support.
        if (columns.Count == 1 && (typeof(T).ToSqlColumnType() is not null || typeof(T).IsEnum))
        {
            var singleColumnCacheKey = new ResultSelectorCacheKey<Type>(typeof(T), columns, defaultIfNull);

            return (RowReader<T>)SingleColumnReaderCache.GetOrAdd(
                singleColumnCacheKey,
                static k => EmitSingleColumnReader<T>(k.Columns[0], k.DefaultIfNull));
        }

        if (typeof(T).GetKeyValuePairTypes() is var (keyType, valType))
        {
            var kvCacheKey = new ResultSelectorCacheKey<(Type Key, Type Val)>((keyType, valType), columns, defaultIfNull);

            return (RowReader<T>)KvReaderCache.GetOrAdd(
                kvCacheKey,
                static k => EmitKvPairReader<T>(k.Columns, k.Target.Key, k.Target.Val, k.DefaultIfNull));
        }

        if (selectorExpression is QuerySourceReferenceExpression
            {
                ReferencedQuerySource: IFromClause { FromExpression: SubQueryExpression subQuery }
            })
        {
            // Select everything from a sub-query - use nested selector.
            return Get<T>(columns, subQuery.QueryModel.SelectClause.Selector, defaultIfNull);
        }

        var readerCacheKey = new ResultSelectorCacheKey<Type>(typeof(T), columns, defaultIfNull);

        return (RowReader<T>)ReaderCache.GetOrAdd(
            readerCacheKey,
            static k => EmitUninitializedObjectReader<T>(k.Columns, k.DefaultIfNull));
    }

    private static RowReader<T> EmitSingleColumnReader<T>(IColumnMetadata column, bool defaultIfNull)
    {
        var method = new DynamicMethod(
            name: $"SingleColumnFromBinaryTupleReader_{typeof(T).FullName}_{GetNextId()}",
            returnType: typeof(T),
            parameterTypes: new[] { typeof(IReadOnlyList<IColumnMetadata>), typeof(BinaryTupleReader).MakeByRefType() },
            m: typeof(IIgnite).Module,
            skipVisibility: true);

        var il = method.GetILGenerator();

        EmitReadToStack(il, column, typeof(T), 0, defaultIfNull);
        il.Emit(OpCodes.Ret);

        return (RowReader<T>)method.CreateDelegate(typeof(RowReader<T>));
    }

    private static RowReader<T> EmitConstructorReader<T>(
        ConstructorInfo ctorInfo,
        IReadOnlyList<IColumnMetadata> columns,
        bool defaultAsNull)
    {
        var method = new DynamicMethod(
            name: $"ConstructorFromBinaryTupleReader_{typeof(T).FullName}_{GetNextId()}",
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

        // Read all constructor parameters and push them to the evaluation stack.
        for (var index = 0; index < ctorParams.Length; index++)
        {
            var paramType = ctorParams[index].ParameterType;
            EmitReadToStack(il, columns[index], paramType, index, defaultAsNull);
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
            name: $"UninitializedObjectFromBinaryTupleReader_{typeof(T).FullName}_{GetNextId()}",
            returnType: typeof(T),
            parameterTypes: new[] { typeof(IReadOnlyList<IColumnMetadata>), typeof(BinaryTupleReader).MakeByRefType() },
            m: typeof(IIgnite).Module,
            skipVisibility: true);

        var il = method.GetILGenerator();

        var resObj = il.DeclareAndInitLocal(typeof(T));

        for (var index = 0; index < columns.Count; index++)
        {
            var col = columns[index];

            EmitFieldRead(il, resObj, col, index, defaultAsNull);
        }

        il.Emit(OpCodes.Ldloc_0); // res
        il.Emit(OpCodes.Ret);

        return (RowReader<T>)method.CreateDelegate(typeof(RowReader<T>));
    }

    private static RowReader<T> EmitKvPairReader<T>(
        IReadOnlyList<IColumnMetadata> columns,
        Type keyType,
        Type valType,
        bool defaultAsNull)
    {
        var method = new DynamicMethod(
            name: $"KvPairFromBinaryTupleReader_{typeof(T).FullName}_{GetNextId()}",
            returnType: typeof(T),
            parameterTypes: new[] { typeof(IReadOnlyList<IColumnMetadata>), typeof(BinaryTupleReader).MakeByRefType() },
            m: typeof(IIgnite).Module,
            skipVisibility: true);

        var il = method.GetILGenerator();

        var key = il.DeclareAndInitLocal(keyType);
        var val = il.DeclareAndInitLocal(valType);

        for (var index = 0; index < columns.Count; index++)
        {
            var col = columns[index];

            EmitFieldRead(il, key, col, index, defaultAsNull);
            EmitFieldRead(il, val, col, index, defaultAsNull);
        }

        il.Emit(OpCodes.Ldloc_0); // key
        il.Emit(OpCodes.Ldloc_1); // val

        il.Emit(OpCodes.Newobj, typeof(T).GetConstructor(new[] { keyType, valType })!);
        il.Emit(OpCodes.Ret);

        return (RowReader<T>)method.CreateDelegate(typeof(RowReader<T>));
    }

    private static void EmitReadToStack(ILGenerator il, IColumnMetadata col, Type targetType, int index, bool defaultAsNull)
    {
        Label endParamLabel = il.DefineLabel();

        if (defaultAsNull)
        {
            // if (reader.IsNull(index)) return default;
            Label notNullLabel = il.DefineLabel();
            il.Emit(OpCodes.Ldarg_1); // Reader.
            il.Emit(OpCodes.Ldc_I4, index); // Index.
            il.Emit(OpCodes.Call, BinaryTupleMethods.IsNull);
            il.Emit(OpCodes.Brfalse_S, notNullLabel);

            if (targetType.IsValueType)
            {
                var local = il.DeclareLocal(targetType);
                il.Emit(OpCodes.Ldloca_S, local);
                il.Emit(OpCodes.Initobj, targetType); // Load default value into local.
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

        il.EmitConv(colType, targetType);
        il.MarkLabel(endParamLabel);
    }

    private static void EmitFieldRead(ILGenerator il, LocalBuilder targetObj, IColumnMetadata col, int colIndex, bool defaultAsNull)
    {
        if (targetObj.LocalType.GetFieldByColumnName(col.Name) is not { } field)
        {
            return;
        }

        Label endFieldLabel = il.DefineLabel();

        if (defaultAsNull)
        {
            // if (reader.IsNull(index)) continue;
            il.Emit(OpCodes.Ldarg_1); // Reader.
            il.Emit(OpCodes.Ldc_I4, colIndex); // Index.
            il.Emit(OpCodes.Call, BinaryTupleMethods.IsNull);
            il.Emit(OpCodes.Brtrue_S, endFieldLabel);
        }

        il.Emit(targetObj.LocalType.IsValueType ? OpCodes.Ldloca_S : OpCodes.Ldloc, targetObj); // res
        il.Emit(OpCodes.Ldarg_1); // Reader.
        il.Emit(OpCodes.Ldc_I4, colIndex); // Index.

        if (col.Type == SqlColumnType.Decimal)
        {
            il.Emit(OpCodes.Ldc_I4, col.Scale);
        }

        var colType = col.Type.ToClrType();
        il.Emit(OpCodes.Call, BinaryTupleMethods.GetReadMethod(colType));

        il.EmitConv(colType, field.FieldType);
        il.Emit(OpCodes.Stfld, field); // res.field = value

        il.MarkLabel(endFieldLabel);
    }

    private static long GetNextId() => Interlocked.Increment(ref _idCounter);
}
