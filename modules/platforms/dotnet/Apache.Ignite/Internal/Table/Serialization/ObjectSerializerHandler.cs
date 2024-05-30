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
    using System.Diagnostics;
    using System.Diagnostics.CodeAnalysis;
    using System.Linq;
    using System.Reflection;
    using System.Reflection.Emit;
    using Common;
    using Ignite.Sql;
    using Proto.BinaryTuple;
    using Proto.MsgPack;
    using Sql;

    /// <summary>
    /// Object serializer handler.
    /// </summary>
    /// <typeparam name="T">Object type.</typeparam>
    internal sealed class ObjectSerializerHandler<T> : IRecordSerializerHandler<T>
    {
        private readonly ConcurrentDictionary<(int, bool), WriteDelegate<T>> _writers = new();

        private readonly ConcurrentDictionary<(int, bool), ReadDelegate<T>> _readers = new();

        [SuppressMessage("Naming", "CA1711:Identifiers should not have incorrect suffix", Justification = "Reviewed.")]
        private delegate void WriteDelegate<in TV>(ref BinaryTupleBuilder writer, Span<byte> noValueSet, TV value);

        [SuppressMessage("Naming", "CA1711:Identifiers should not have incorrect suffix", Justification = "Reviewed.")]
        private delegate TV ReadDelegate<out TV>(ref BinaryTupleReader reader);

        /// <inheritdoc/>
        public T Read(ref MsgPackReader reader, Schema schema, bool keyOnly = false)
        {
            var cacheKey = (schema.Version, keyOnly);

            var readDelegate = _readers.TryGetValue(cacheKey, out var w)
                ? w
                : _readers.GetOrAdd(cacheKey, EmitReader(schema, keyOnly));

            var columnCount = keyOnly ? schema.KeyColumns.Length : schema.Columns.Length;

            var binaryTupleReader = new BinaryTupleReader(reader.ReadBinary(), columnCount);

            return readDelegate(ref binaryTupleReader);
        }

        /// <inheritdoc/>
        public void Write(ref BinaryTupleBuilder tupleBuilder, T record, Schema schema, bool keyOnly, Span<byte> noValueSet)
        {
            var cacheKey = (schema.Version, keyOnly);

            var writeDelegate = _writers.TryGetValue(cacheKey, out var w)
                ? w
                : _writers.GetOrAdd(cacheKey, EmitWriter(schema, keyOnly));

            writeDelegate(ref tupleBuilder, noValueSet, record);
        }

        private static WriteDelegate<T> EmitWriter(Schema schema, bool keyOnly)
        {
            var type = typeof(T);

            var method = new DynamicMethod(
                name: "Write" + type,
                returnType: typeof(void),
                parameterTypes: new[] { typeof(BinaryTupleBuilder).MakeByRefType(), typeof(Span<byte>), type },
                m: typeof(IIgnite).Module,
                skipVisibility: true);

            if (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(KvPair<,>))
            {
                return EmitKvWriter(schema, keyOnly, method);
            }

            var il = method.GetILGenerator();

            var columns = schema.GetColumnsFor(keyOnly);
            var columnMap = type.GetFieldsByColumnName();

            if (BinaryTupleMethods.GetWriteMethodOrNull(type) is { } directWriteMethod)
            {
                // Single column to primitive type mapping.
                var col = columns[0];
                ValidateSingleFieldMappingType(type, col);

                il.Emit(OpCodes.Ldarg_0); // writer
                il.Emit(OpCodes.Ldarg_2); // value

                if (col.Type == ColumnType.Decimal)
                {
                    il.Emit(OpCodes.Ldc_I4, col.Scale);
                }
                else if (col.Type is ColumnType.Time or ColumnType.Datetime or ColumnType.Timestamp)
                {
                    il.Emit(OpCodes.Ldc_I4, col.Precision);
                }

                il.Emit(OpCodes.Call, directWriteMethod);

                for (var index = 1; index < columns.Length; index++)
                {
                    il.Emit(OpCodes.Ldarg_0); // writer
                    il.Emit(OpCodes.Ldarg_1); // noValueSet
                    il.Emit(OpCodes.Call, BinaryTupleMethods.WriteNoValue);
                }

                il.Emit(OpCodes.Ret);

                return (WriteDelegate<T>)method.CreateDelegate(typeof(WriteDelegate<T>));
            }

            int mappedCount = 0;

            foreach (var col in columns)
            {
                var fieldInfo = columnMap.TryGetValue(col.Name, out var columnInfo) ? columnInfo.Field : null;

                if (fieldInfo == null)
                {
                    il.Emit(OpCodes.Ldarg_0); // writer
                    il.Emit(OpCodes.Ldarg_1); // noValueSet
                    il.Emit(OpCodes.Call, BinaryTupleMethods.WriteNoValue);
                }
                else
                {
                    ValidateFieldType(fieldInfo, col);
                    il.Emit(OpCodes.Ldarg_0); // writer
                    il.Emit(OpCodes.Ldarg_2); // record
                    il.Emit(OpCodes.Ldfld, fieldInfo);

                    if (col.Type == ColumnType.Decimal)
                    {
                        il.Emit(OpCodes.Ldc_I4, col.Scale);
                    }
                    else if (col.Type is ColumnType.Time or ColumnType.Datetime or ColumnType.Timestamp)
                    {
                        il.Emit(OpCodes.Ldc_I4, col.Precision);
                    }

                    var writeMethod = BinaryTupleMethods.GetWriteMethod(fieldInfo.FieldType);
                    il.Emit(OpCodes.Call, writeMethod);

                    mappedCount++;
                }
            }

            ValidateMappedCount(mappedCount, type, schema, keyOnly);

            il.Emit(OpCodes.Ret);

            return (WriteDelegate<T>)method.CreateDelegate(typeof(WriteDelegate<T>));
        }

        private static WriteDelegate<T> EmitKvWriter(Schema schema, bool keyOnly, DynamicMethod method)
        {
            var (keyType, valType, keyField, valField, keyColumnMap, valColumnMap) = GetKeyValTypes();

            var keyWriteMethod = BinaryTupleMethods.GetWriteMethodOrNull(keyType);
            var valWriteMethod = BinaryTupleMethods.GetWriteMethodOrNull(valType);

            var il = method.GetILGenerator();

            var columns = schema.GetColumnsFor(keyOnly);

            int mappedCount = 0;

            foreach (var col in columns)
            {
                FieldInfo? fieldInfo;

                if (keyWriteMethod != null && col.IsKey)
                {
                    fieldInfo = keyField;
                }
                else if (valWriteMethod != null && !col.IsKey)
                {
                    fieldInfo = valField;
                }
                else if ((col.IsKey && keyWriteMethod != null) || (!col.IsKey && valWriteMethod != null))
                {
                    fieldInfo = null;
                }
                else
                {
                    fieldInfo = (col.IsKey ? keyColumnMap : valColumnMap).TryGetValue(col.Name, out var columnInfo)
                        ? columnInfo.Field
                        : null;
                }

                if (fieldInfo == null)
                {
                    il.Emit(OpCodes.Ldarg_0); // writer
                    il.Emit(OpCodes.Ldarg_1); // noValueSet
                    il.Emit(OpCodes.Call, BinaryTupleMethods.WriteNoValue);
                }
                else
                {
                    ValidateFieldType(fieldInfo, col);

                    var field = col.IsKey ? keyField : valField;

                    if (field != fieldInfo)
                    {
                        // POCO mapping: emit null check (nulls are allowed for single-column simple type mapping like KvPair<int, long?>).
                        il.Emit(OpCodes.Ldarg_2); // record
                        il.Emit(OpCodes.Ldfld, field);
                        il.Emit(OpCodes.Ldstr, field == keyField ? "key" : "val");

                        il.Emit(OpCodes.Call, IgniteArgumentCheck.NotNullMethod.MakeGenericMethod(field.FieldType));
                    }

                    il.Emit(OpCodes.Ldarg_0); // writer
                    il.Emit(OpCodes.Ldarg_2); // record

                    il.Emit(OpCodes.Ldfld, field);

                    if (field != fieldInfo)
                    {
                        il.Emit(OpCodes.Ldfld, fieldInfo);
                    }

                    if (col.Type == ColumnType.Decimal)
                    {
                        il.Emit(OpCodes.Ldc_I4, col.Scale);
                    }
                    else if (col.Type is ColumnType.Time or ColumnType.Datetime or ColumnType.Timestamp)
                    {
                        il.Emit(OpCodes.Ldc_I4, col.Precision);
                    }

                    var writeMethod = BinaryTupleMethods.GetWriteMethod(fieldInfo.FieldType);
                    il.Emit(OpCodes.Call, writeMethod);

                    mappedCount++;
                }
            }

            ValidateKvMappedCount(
                mappedCount,
                keyWriteMethod != null ? null : keyType,
                valWriteMethod != null ? null : valType,
                schema,
                keyOnly);

            il.Emit(OpCodes.Ret);

            return (WriteDelegate<T>)method.CreateDelegate(typeof(WriteDelegate<T>));
        }

        private static ReadDelegate<T> EmitReader(Schema schema, bool keyOnly)
        {
            var type = typeof(T);

            var method = new DynamicMethod(
                name: "Read" + type,
                returnType: type,
                parameterTypes: new[] { typeof(BinaryTupleReader).MakeByRefType() },
                m: typeof(IIgnite).Module,
                skipVisibility: true);

            if (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(KvPair<,>))
            {
                return EmitKvReader(schema, keyOnly, method);
            }

            var il = method.GetILGenerator();

            if (BinaryTupleMethods.GetReadMethodOrNull(type) is { } readMethod)
            {
                // Single column to primitive type mapping.
                il.Emit(OpCodes.Ldarg_0); // reader
                il.Emit(OpCodes.Ldc_I4_0); // index

                il.Emit(OpCodes.Call, readMethod);
                il.Emit(OpCodes.Ret);

                return (ReadDelegate<T>)method.CreateDelegate(typeof(ReadDelegate<T>));
            }

            var local = il.DeclareAndInitLocal(type);

            var columns = schema.GetColumnsFor(keyOnly);
            var columnMap = type.GetFieldsByColumnName();

            foreach (var col in columns)
            {
                var fieldInfo = columnMap.TryGetValue(col.Name, out var columnInfo) ? columnInfo.Field : null;
                var idx = col.GetBinaryTupleIndex(keyOnly);

                EmitFieldRead(fieldInfo, il, col, idx, local);
            }

            il.Emit(OpCodes.Ldloc_0); // res
            il.Emit(OpCodes.Ret);

            return (ReadDelegate<T>)method.CreateDelegate(typeof(ReadDelegate<T>));
        }

        private static ReadDelegate<T> EmitKvReader(Schema schema, bool keyOnly, DynamicMethod method)
        {
            var type = typeof(T);

            var il = method.GetILGenerator();
            var (keyType, valType, keyField, valField, keyColumnMap, valColumnMap) = GetKeyValTypes();

            var keyMethod = BinaryTupleMethods.GetReadMethodOrNull(keyType);
            var valMethod = BinaryTupleMethods.GetReadMethodOrNull(valType);

            var kvLocal = il.DeclareAndInitLocal(type);
            var keyLocal = keyMethod == null ? il.DeclareAndInitLocal(keyType) : null;
            var valLocal = valMethod == null ? il.DeclareAndInitLocal(valType) : null;

            var columns = schema.GetColumnsFor(keyOnly);

            foreach (var col in columns)
            {
                FieldInfo? fieldInfo;
                LocalBuilder? local;

                if (col.IsKey && keyMethod != null)
                {
                    fieldInfo = keyField;
                    local = kvLocal;
                    ValidateSingleFieldMappingType(keyType, col);
                }
                else if (!col.IsKey && valMethod != null)
                {
                    fieldInfo = valField;
                    local = kvLocal;
                    ValidateSingleFieldMappingType(valType, col);
                }
                else
                {
                    local = col.IsKey ? keyLocal : valLocal;
                    fieldInfo = local == null
                        ? null
                        : (col.IsKey ? keyColumnMap : valColumnMap).TryGetValue(col.Name, out var columnInfo)
                            ? columnInfo.Field
                            : null;
                }

                var idx = col.GetBinaryTupleIndex(keyOnly);
                EmitFieldRead(fieldInfo, il, col, idx, local);
            }

            // Copy Key to KvPair.
            if (keyLocal != null)
            {
                il.Emit(OpCodes.Ldloca_S, kvLocal);
                il.Emit(OpCodes.Ldloc, keyLocal);
                il.Emit(OpCodes.Stfld, keyField);
            }

            // Copy Val to KvPair.
            if (valLocal != null)
            {
                il.Emit(OpCodes.Ldloca_S, kvLocal);
                il.Emit(OpCodes.Ldloc, valLocal);
                il.Emit(OpCodes.Stfld, valField);
            }

            il.Emit(OpCodes.Ldloc_0); // res
            il.Emit(OpCodes.Ret);

            return (ReadDelegate<T>)method.CreateDelegate(typeof(ReadDelegate<T>));
        }

        private static void EmitFieldRead(FieldInfo? fieldInfo, ILGenerator il, Column col, int elemIdx, LocalBuilder? local)
        {
            if (fieldInfo == null || local == null)
            {
                return;
            }

            ValidateFieldType(fieldInfo, col);

            var readMethod = BinaryTupleMethods.GetReadMethod(fieldInfo.FieldType);

            il.Emit(local.LocalType.IsValueType ? OpCodes.Ldloca_S : OpCodes.Ldloc, local); // res
            il.Emit(OpCodes.Ldarg_0); // reader
            il.Emit(OpCodes.Ldc_I4, elemIdx); // index

            il.Emit(OpCodes.Call, readMethod);
            il.Emit(OpCodes.Stfld, fieldInfo); // res.field = value
        }

        private static void ValidateFieldType(FieldInfo fieldInfo, Column column)
        {
            var columnType = column.Type.ToClrType();
            var type = fieldInfo.FieldType;

            bool typeIsNullable = !type.IsValueType;
            if (!typeIsNullable && Nullable.GetUnderlyingType(type) is {} nullableType)
            {
                typeIsNullable = true;
                type = nullableType;
            }

            type = type.UnwrapEnum();

            if (type != columnType)
            {
                var message = $"Can't map field '{fieldInfo.DeclaringType?.Name}.{fieldInfo.Name}' of type '{fieldInfo.FieldType}' " +
                              $"to column '{column.Name}' of type '{columnType}' - types do not match.";

                throw new IgniteClientException(ErrorGroups.Client.Configuration, message);
            }

            if (column.IsNullable && !typeIsNullable)
            {
                var message = $"Can't map field '{fieldInfo.DeclaringType?.Name}.{fieldInfo.Name}' of type '{fieldInfo.FieldType}' " +
                              $"to column '{column.Name}' - column is nullable, but field is not.";

                throw new IgniteClientException(ErrorGroups.Client.Configuration, message);
            }
        }

        private static void ValidateSingleFieldMappingType(Type type, Column column)
        {
            var columnType = column.Type.ToClrType();

            bool typeIsNullable = !type.IsValueType;
            if (!typeIsNullable && Nullable.GetUnderlyingType(type) is {} nullableType)
            {
                typeIsNullable = true;
                type = nullableType;
            }

            if (type != columnType)
            {
                var message = $"Can't map '{type}' to column '{column.Name}' of type '{columnType}' - types do not match.";

                throw new IgniteClientException(ErrorGroups.Client.Configuration, message);
            }

            if (column.IsNullable && !typeIsNullable)
            {
                var message = $"Can't map '{type}' to column '{column.Name}' - column is nullable, but field is not.";

                throw new IgniteClientException(ErrorGroups.Client.Configuration, message);
            }
        }

        private static void ValidateMappedCount(int mappedCount, Type type, Schema schema, bool keyOnly)
        {
            if (mappedCount == 0)
            {
                var columnStr = schema.Columns.Select(x => x.Type + " " + x.Name).StringJoin();
                throw new ArgumentException($"Can't map '{type}' to columns '{columnStr}'. Matching fields not found.");
            }

            if (keyOnly)
            {
                // Key-only mode - skip "all fields are mapped" validation.
                // It will be performed anyway when using the whole schema.
                return;
            }

            var fields = type.GetColumns();

            if (fields.Count > mappedCount)
            {
                var extraColumns = new HashSet<string>(fields.Count, StringComparer.OrdinalIgnoreCase);

                foreach (var field in fields)
                {
                    extraColumns.Add(field.Name);
                }

                foreach (var column in schema.GetColumnsFor(keyOnly))
                {
                    extraColumns.Remove(column.Name);
                }

                throw SerializerExceptionExtensions.GetUnmappedColumnsException($"Record of type {type}", schema, extraColumns);
            }
        }

        private static void ValidateKvMappedCount(int mappedCount, Type? keyType, Type? valType, Schema schema, bool keyOnly)
        {
            if (mappedCount == 0)
            {
                var columnStr = schema.Columns.Select(x => x.Type + " " + x.Name).StringJoin();
                throw new ArgumentException($"Can't map '{keyType}' and '{valType}' to columns '{columnStr}'. Matching fields not found.");
            }

            var keyFields = keyType?.GetColumns() ?? Array.Empty<ReflectionUtils.ColumnInfo>();
            var valFields = valType != null && !keyOnly
                ? valType.GetColumns()
                : Array.Empty<ReflectionUtils.ColumnInfo>();

            if (keyFields.Count + valFields.Count > mappedCount)
            {
                var extraColumns = new HashSet<string>(keyFields.Count + valFields.Count, StringComparer.OrdinalIgnoreCase);

                foreach (var field in keyFields)
                {
                    extraColumns.Add(field.Name);
                }

                foreach (var field in valFields)
                {
                    if (extraColumns.Contains(field.Name))
                    {
                        throw new ArgumentException(
                            $"Duplicate field in Value portion of KeyValue pair ({keyType}, {valType}): " + field.Name);
                    }

                    extraColumns.Add(field.Name);
                }

                foreach (var column in schema.GetColumnsFor(keyOnly))
                {
                    extraColumns.Remove(column.Name);
                }

                throw SerializerExceptionExtensions.GetUnmappedColumnsException(
                    $"KeyValue pair of type ({keyType}, {valType})", schema, extraColumns);
            }
        }

        private static (
            Type KeyType,
            Type ValType,
            FieldInfo KeyField,
            FieldInfo ValField,
            IReadOnlyDictionary<string, ReflectionUtils.ColumnInfo> KeyColumnMap,
            IReadOnlyDictionary<string, ReflectionUtils.ColumnInfo> ValColumnMap) GetKeyValTypes()
        {
            var type = typeof(T);
            Debug.Assert(
                type.IsGenericType && type.GetGenericTypeDefinition() == typeof(KvPair<,>),
                "type.IsGenericType && type.GetGenericTypeDefinition() == typeof(KvPair<,>)");

            var keyValTypes = type.GetGenericArguments();
            var keyColumnMap = keyValTypes[0].GetFieldsByColumnName();
            var valColumnMap = keyValTypes[1].GetFieldsByColumnName();
            var columnMap = type.GetFieldsByColumnName();

            return (keyValTypes[0], keyValTypes[1], columnMap["Key"].Field, columnMap["Val"].Field, keyColumnMap, valColumnMap);
        }
    }
}
