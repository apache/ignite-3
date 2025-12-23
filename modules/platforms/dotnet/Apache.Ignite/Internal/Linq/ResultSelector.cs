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
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Reflection.Emit;
using System.Threading;
using Common;
using Ignite.Sql;
using Proto.BinaryTuple;
using Remotion.Linq.Clauses;
using Remotion.Linq.Clauses.Expressions;
using Sql;
using Table.Serialization;

using static ResultSelectorOptions;

/// <summary>
/// Result selector cache.
/// </summary>
[RequiresUnreferencedCode(IgniteQueryExecutor.TrimWarning)]
internal static class ResultSelector
{
    private static readonly ConcurrentDictionary<ResultSelectorCacheKey<ConstructorInfo>, object> CtorCache = new();

    private static readonly ConcurrentDictionary<ResultSelectorCacheKey<MemberInitCacheTarget>, object> MemberInitCache = new();

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
    /// <param name="meta">Metadata.</param>
    /// <param name="selectorExpression">Selector expression.</param>
    /// <param name="options">Options.</param>
    /// <typeparam name="T">Result type.</typeparam>
    /// <returns>Row reader.</returns>
    public static RowReader<T> Get<T>(ResultSetMetadata meta, Expression? selectorExpression, ResultSelectorOptions options)
    {
        var columns = meta.Columns;

        // Anonymous type projections use a constructor call. But user-defined types can also be used with constructor call.
        if (selectorExpression is NewExpression newExpr)
        {
            var ctorInfo = newExpr.Constructor!;
            var ctorCacheKey = new ResultSelectorCacheKey<ConstructorInfo>(ctorInfo, columns, options);

            return (RowReader<T>)CtorCache.GetOrAdd(
                ctorCacheKey,
                static k => EmitConstructorReader<T>(k.Target, k.Columns, k.Options));
        }

        if (selectorExpression is MemberInitExpression memberInitExpression)
        {
            var ctorInfo = memberInitExpression.NewExpression.Constructor!;
            var cacheTarget = new MemberInitCacheTarget(ctorInfo, memberInitExpression.Bindings);
            var ctorCacheKey = new ResultSelectorCacheKey<MemberInitCacheTarget>(cacheTarget, columns, options);
            return (RowReader<T>)MemberInitCache.GetOrAdd(
                    ctorCacheKey,
                    static k => EmitMemberInitReader<T>(k.Target, k.Columns, k.Options));
        }

        if (columns.Count == 1 && (typeof(T).ToColumnType() is not null || typeof(T).IsEnum))
        {
            var singleColumnCacheKey = new ResultSelectorCacheKey<Type>(typeof(T), columns, options);

            return (RowReader<T>)SingleColumnReaderCache.GetOrAdd(
                singleColumnCacheKey,
                static k => EmitSingleColumnReader<T>(k.Columns[0], k.Options));
        }

        if (typeof(T).GetKeyValuePairTypes() is var (keyType, valType))
        {
            var kvCacheKey = new ResultSelectorCacheKey<(Type Key, Type Val)>((keyType, valType), columns, options);

            return (RowReader<T>)KvReaderCache.GetOrAdd(
                kvCacheKey,
                static k =>
                    EmitKvPairReader<T>(k.Columns, k.Target.Key, k.Target.Val, k.Options == ReturnDefaultIfNull));
        }

        if (selectorExpression is QuerySourceReferenceExpression
            {
                ReferencedQuerySource: IFromClause { FromExpression: SubQueryExpression subQuery }
            })
        {
            // Select everything from a sub-query - use nested selector.
            return Get<T>(meta, subQuery.QueryModel.SelectClause.Selector, options);
        }

        var readerCacheKey = new ResultSelectorCacheKey<Type>(typeof(T), columns, options);

        return (RowReader<T>)ReaderCache.GetOrAdd(
            readerCacheKey,
            static k => EmitUninitializedObjectReader<T>(k.Columns, k.Options == ReturnDefaultIfNull));
    }

    private static RowReader<T> EmitSingleColumnReader<T>(IColumnMetadata column, ResultSelectorOptions options)
    {
        var method = new DynamicMethod(
            name: $"SingleColumnFromBinaryTupleReader_{typeof(T).FullName}_{GetNextId()}",
            returnType: typeof(T),
            parameterTypes: [typeof(ResultSetMetadata), typeof(BinaryTupleReader).MakeByRefType(), typeof(object)],
            m: typeof(IIgnite).Module,
            skipVisibility: true);

        var il = method.GetILGenerator();

        EmitReadToStack(il, column, typeof(T), index: 0, options);

        il.Emit(OpCodes.Ret);

        return (RowReader<T>)method.CreateDelegate(typeof(RowReader<T>));
    }

    private static RowReader<T> EmitConstructorReader<T>(
        ConstructorInfo ctorInfo,
        IReadOnlyList<IColumnMetadata> columns,
        ResultSelectorOptions options)
    {
        var method = new DynamicMethod(
            name: $"ConstructorFromBinaryTupleReader_{typeof(T).FullName}_{GetNextId()}",
            returnType: typeof(T),
            parameterTypes: [typeof(ResultSetMetadata), typeof(BinaryTupleReader).MakeByRefType(), typeof(object)],
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
            EmitReadToStack(il, columns[index], paramType, index, options);
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
            parameterTypes: [typeof(ResultSetMetadata), typeof(BinaryTupleReader).MakeByRefType(), typeof(object)],
            m: typeof(IIgnite).Module,
            skipVisibility: true);

        var il = method.GetILGenerator();
        var resObj = il.DeclareAndInitLocal(typeof(T));
        var colMap = typeof(T).GetFieldsByColumnName();

        int mappedCount = 0;

        for (var index = 0; index < columns.Count; index++)
        {
            var col = columns[index];

            if (EmitFieldRead(il, resObj, colMap, col, index, defaultAsNull))
            {
                mappedCount++;
            }
        }

        ValidateMappedCount(mappedCount, typeof(T), columns);

        il.Emit(OpCodes.Ldloc_0); // res
        il.Emit(OpCodes.Ret);

        return (RowReader<T>)method.CreateDelegate(typeof(RowReader<T>));
    }

    private static RowReader<T> EmitMemberInitReader<T>(
        MemberInitCacheTarget target,
        IReadOnlyList<IColumnMetadata> columns,
        ResultSelectorOptions options)
    {
        var method = new DynamicMethod(
            name: $"MemberInitFromBinaryTupleReader_{typeof(T).FullName}_{GetNextId()}",
            returnType: typeof(T),
            parameterTypes: [typeof(ResultSetMetadata), typeof(BinaryTupleReader).MakeByRefType(), typeof(object)],
            m: typeof(IIgnite).Module,
            skipVisibility: true);

        var il = method.GetILGenerator();
        var ctorParams = target.CtorInfo.GetParameters();
        var memberBindings = target.MemberBindings;

        if (ctorParams.Length + memberBindings.Count != columns.Count)
        {
            throw new InvalidOperationException("Constructor parameter count + initialized members parameter count" +
                                                " does not match column count, can't emit row reader.");
        }

        // Read all constructor parameters and push them to the evaluation stack.
        var columnsIndex = 0;
        for (; columnsIndex < ctorParams.Length; columnsIndex++)
        {
            var paramType = ctorParams[columnsIndex].ParameterType;
            EmitReadToStack(il, columns[columnsIndex], paramType, columnsIndex, options);
        }

        // create the object
        il.Emit(OpCodes.Newobj, target.CtorInfo);

        // initialize the members
        for (int memberIndex = 0; memberIndex < memberBindings.Count; memberIndex++, columnsIndex++)
        {
            il.Emit(OpCodes.Dup);
            var member = memberBindings[memberIndex].Member;

            if (member is PropertyInfo {SetMethod: {}} propertyInfo)
            {
                EmitReadToStack(il, columns[columnsIndex], propertyInfo.PropertyType, columnsIndex, options);
                il.Emit(OpCodes.Callvirt, propertyInfo.SetMethod);
            }
            else if (member is FieldInfo fieldInfo)
            {
                EmitReadToStack(il, columns[columnsIndex], fieldInfo.FieldType, columnsIndex, options);
                il.Emit(OpCodes.Stfld, fieldInfo);
            }
            else
            {
                throw new InvalidOperationException($"Member type {member.GetType()} is not supported.");
            }
        }

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

        var keyColMap = keyType.GetFieldsByColumnName();
        var valColMap = valType.GetFieldsByColumnName();

        for (var index = 0; index < columns.Count; index++)
        {
            var col = columns[index];

            EmitFieldRead(il, key, keyColMap, col, index, defaultAsNull);
            EmitFieldRead(il, val, valColMap, col, index, defaultAsNull);
        }

        il.Emit(OpCodes.Ldloc_0); // key
        il.Emit(OpCodes.Ldloc_1); // val

        il.Emit(OpCodes.Newobj, typeof(T).GetConstructor(new[] { keyType, valType })!);
        il.Emit(OpCodes.Ret);

        return (RowReader<T>)method.CreateDelegate(typeof(RowReader<T>));
    }

    private static void EmitReadToStack(ILGenerator il, IColumnMetadata col, Type targetType, int index, ResultSelectorOptions options)
    {
        Label endParamLabel = il.DefineLabel();

        if (options is ReturnDefaultIfNull or ThrowNoElementsIfNull or ReturnZeroIfNull)
        {
            // if (reader.IsNull(index)) return default;
            Label notNullLabel = il.DefineLabel();
            il.Emit(OpCodes.Ldarg_1); // Reader.
            il.Emit(OpCodes.Ldc_I4, index); // Index.
            il.Emit(OpCodes.Call, BinaryTupleMethods.IsNull);
            il.Emit(OpCodes.Brfalse_S, notNullLabel);

            if (options is ReturnDefaultIfNull or ReturnZeroIfNull)
            {
                if (targetType.IsValueType)
                {
                    if (options == ReturnZeroIfNull && Nullable.GetUnderlyingType(targetType) is { } underlyingType)
                    {
                        // Create nullable with default non-zero value.
                        var local = il.DeclareLocal(underlyingType);
                        il.Emit(OpCodes.Ldloca_S, local);
                        il.Emit(OpCodes.Initobj, underlyingType); // Load default value into local.
                        il.Emit(OpCodes.Ldloc, local); // Load local value onto stack for constructor call.
                        il.Emit(OpCodes.Newobj, targetType.GetConstructor(new[] { underlyingType })!);
                    }
                    else
                    {
                        var local = il.DeclareLocal(targetType);
                        il.Emit(OpCodes.Ldloca_S, local);
                        il.Emit(OpCodes.Initobj, targetType); // Load default value into local.
                        il.Emit(OpCodes.Ldloc, local); // Load local value onto stack for constructor call.
                    }
                }
                else
                {
                    il.Emit(OpCodes.Ldnull);
                }
            }
            else
            {
                il.Emit(OpCodes.Ldstr, "Sequence contains no elements");
                il.Emit(OpCodes.Newobj, typeof(InvalidOperationException).GetConstructor(new[] { typeof(string) })!);
                il.Emit(OpCodes.Throw);
            }

            il.Emit(OpCodes.Br_S, endParamLabel);
            il.MarkLabel(notNullLabel);
        }

        il.Emit(OpCodes.Ldarg_1); // Reader.
        il.Emit(OpCodes.Ldc_I4, index); // Index.

        if (col.Type == ColumnType.Decimal)
        {
            il.Emit(OpCodes.Ldc_I4, col.Scale);
        }

        var colType = col.Type.ToClrType(col.Nullable);
        var readMethod = BinaryTupleMethods.GetReadMethod(colType, targetType);
        il.Emit(OpCodes.Call, readMethod);

        il.EmitConv(readMethod.ReturnType, targetType, col.Name);
        il.MarkLabel(endParamLabel);
    }

    private static bool EmitFieldRead(
        ILGenerator il,
        LocalBuilder targetObj,
        IReadOnlyDictionary<string, ReflectionUtils.ColumnInfo> columnMap,
        IColumnMetadata col,
        int colIndex,
        bool defaultAsNull)
    {
        if (!columnMap.TryGetValue(col.Name, out var columnInfo))
        {
            return false;
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

        if (col.Type == ColumnType.Decimal)
        {
            il.Emit(OpCodes.Ldc_I4, col.Scale);
        }

        var colType = col.Type.ToClrType(col.Nullable);
        var readMethod = BinaryTupleMethods.GetReadMethod(colType, columnInfo.Field.FieldType);
        il.Emit(OpCodes.Call, readMethod);

        il.EmitConv(readMethod.ReturnType, columnInfo.Field.FieldType, col.Name);
        il.Emit(OpCodes.Stfld, columnInfo.Field); // res.field = value

        il.MarkLabel(endFieldLabel);

        return true;
    }

    private static void ValidateMappedCount(int mappedCount, Type type, IEnumerable<IColumnMetadata> columns)
    {
        if (mappedCount > 0)
        {
            return;
        }

        var columnStr = columns.Select(x => x.Type + " " + x.Name).StringJoin();

        throw new IgniteClientException(
            ErrorGroups.Client.Configuration,
            $"Can't map '{type}' to columns '{columnStr}'. Matching fields not found.");
    }

    private static long GetNextId() => Interlocked.Increment(ref _idCounter);
}
