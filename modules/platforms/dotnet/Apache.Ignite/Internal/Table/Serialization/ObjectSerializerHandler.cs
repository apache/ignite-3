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
    using MessagePack;
    using Proto;
    using Proto.BinaryTuple;

    /// <summary>
    /// Object serializer handler.
    /// </summary>
    /// <typeparam name="T">Object type.</typeparam>
    internal class ObjectSerializerHandler<T> : IRecordSerializerHandler<T>
    {
        private readonly ConcurrentDictionary<(int, int), WriteDelegate<T>> _writers = new();

        private readonly ConcurrentDictionary<(int, bool), ReadDelegate<T>> _readers = new();

        private readonly ConcurrentDictionary<int, ReadValuePartDelegate<T>> _valuePartReaders = new();

        [SuppressMessage("Naming", "CA1711:Identifiers should not have incorrect suffix", Justification = "Reviewed.")]
        private delegate void WriteDelegate<in TV>(ref BinaryTupleBuilder writer, Span<byte> noValueSet, TV value);

        [SuppressMessage("Naming", "CA1711:Identifiers should not have incorrect suffix", Justification = "Reviewed.")]
        private delegate TV ReadDelegate<out TV>(ref BinaryTupleReader reader);

        [SuppressMessage("Naming", "CA1711:Identifiers should not have incorrect suffix", Justification = "Reviewed.")]
        private delegate TV ReadValuePartDelegate<TV>(ref BinaryTupleReader reader, TV key);

        /// <inheritdoc/>
        public T Read(ref MessagePackReader reader, Schema schema, bool keyOnly = false)
        {
            var cacheKey = (schema.Version, keyOnly);

            var readDelegate = _readers.TryGetValue(cacheKey, out var w)
                ? w
                : _readers.GetOrAdd(cacheKey, EmitReader(schema, keyOnly));

            var columnCount = keyOnly ? schema.KeyColumnCount : schema.Columns.Count;

            var binaryTupleReader = new BinaryTupleReader(reader.ReadBytesAsSpan(), columnCount);

            return readDelegate(ref binaryTupleReader);
        }

        /// <inheritdoc/>
        public T ReadValuePart(ref MessagePackReader reader, Schema schema, T key)
        {
            var readDelegate = _valuePartReaders.TryGetValue(schema.Version, out var w)
                ? w
                : _valuePartReaders.GetOrAdd(schema.Version, EmitValuePartReader(schema));

            var binaryTupleReader = new BinaryTupleReader(reader.ReadBytesAsSpan(), schema.ValueColumnCount);

            return readDelegate(ref binaryTupleReader, key);
        }

        /// <inheritdoc/>
        public void Write(ref BinaryTupleBuilder tupleBuilder, T record, Schema schema, int columnCount, Span<byte> noValueSet)
        {
            var cacheKey = (schema.Version, columnCount);

            var writeDelegate = _writers.TryGetValue(cacheKey, out var w)
                ? w
                : _writers.GetOrAdd(cacheKey, EmitWriter(schema, columnCount));

            writeDelegate(ref tupleBuilder, noValueSet, record);
        }

        private static WriteDelegate<T> EmitWriter(Schema schema, int count)
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
                return EmitKvWriter(schema, count, method);
            }

            var il = method.GetILGenerator();

            var columns = schema.Columns;

            if (BinaryTupleMethods.GetWriteMethodOrNull(type) is { } directWriteMethod)
            {
                // Single column to primitive type mapping.
                var col = columns[0];
                ValidateSingleFieldMappingType(type, col);

                il.Emit(OpCodes.Ldarg_0); // writer
                il.Emit(OpCodes.Ldarg_2); // value

                if (col.Type == ClientDataType.Decimal)
                {
                    il.Emit(OpCodes.Ldc_I4, col.Scale);
                }

                il.Emit(OpCodes.Call, directWriteMethod);

                for (var index = 1; index < count; index++)
                {
                    il.Emit(OpCodes.Ldarg_0); // writer
                    il.Emit(OpCodes.Ldarg_1); // noValueSet
                    il.Emit(OpCodes.Call, BinaryTupleMethods.WriteNoValue);
                }

                il.Emit(OpCodes.Ret);

                return (WriteDelegate<T>)method.CreateDelegate(typeof(WriteDelegate<T>));
            }

            int mappedCount = 0;

            for (var index = 0; index < count; index++)
            {
                var col = columns[index];
                var fieldInfo = type.GetFieldByColumnName(col.Name);

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

                    if (col.Type == ClientDataType.Decimal)
                    {
                        il.Emit(OpCodes.Ldc_I4, col.Scale);
                    }

                    var writeMethod = BinaryTupleMethods.GetWriteMethod(fieldInfo.FieldType);
                    il.Emit(OpCodes.Call, writeMethod);

                    mappedCount++;
                }
            }

            ValidateMappedCount(mappedCount, type, columns);

            il.Emit(OpCodes.Ret);

            return (WriteDelegate<T>)method.CreateDelegate(typeof(WriteDelegate<T>));
        }

        private static WriteDelegate<T> EmitKvWriter(Schema schema, int count, DynamicMethod method)
        {
            var type = typeof(T);
            var (keyType, valType, keyField, valField) = GetKeyValTypes();

            var keyWriteMethod = BinaryTupleMethods.GetWriteMethodOrNull(keyType);
            var valWriteMethod = BinaryTupleMethods.GetWriteMethodOrNull(valType);

            var il = method.GetILGenerator();

            var columns = schema.Columns;

            int mappedCount = 0;

            for (var index = 0; index < count; index++)
            {
                var col = columns[index];

                FieldInfo? fieldInfo;

                if (keyWriteMethod != null && index == 0)
                {
                    fieldInfo = keyField;
                }
                else if (valWriteMethod != null && index == schema.KeyColumnCount)
                {
                    fieldInfo = valField;
                }
                else if ((col.IsKey && keyWriteMethod != null) || (!col.IsKey && valWriteMethod != null))
                {
                    fieldInfo = null;
                }
                else
                {
                    fieldInfo = (col.IsKey ? keyType : valType).GetFieldByColumnName(col.Name);
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
                    il.Emit(OpCodes.Ldarg_0); // writer
                    il.Emit(OpCodes.Ldarg_2); // record

                    var field = index < schema.KeyColumnCount ? keyField : valField;
                    il.Emit(OpCodes.Ldfld, field);

                    if (field != fieldInfo)
                    {
                        il.Emit(OpCodes.Ldfld, fieldInfo);
                    }

                    if (col.Type == ClientDataType.Decimal)
                    {
                        il.Emit(OpCodes.Ldc_I4, col.Scale);
                    }

                    var writeMethod = BinaryTupleMethods.GetWriteMethod(fieldInfo.FieldType);
                    il.Emit(OpCodes.Call, writeMethod);

                    mappedCount++;
                }
            }

            ValidateMappedCount(mappedCount, type, columns);

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

                if (schema.Columns[0] is { Type: ClientDataType.Decimal } col)
                {
                    il.Emit(OpCodes.Ldc_I4, col.Scale);
                }

                il.Emit(OpCodes.Call, readMethod);
                il.Emit(OpCodes.Ret);

                return (ReadDelegate<T>)method.CreateDelegate(typeof(ReadDelegate<T>));
            }

            var local = il.DeclareAndInitLocal(type);

            var columns = schema.Columns;
            var count = keyOnly ? schema.KeyColumnCount : columns.Count;

            for (var i = 0; i < count; i++)
            {
                var col = columns[i];
                var fieldInfo = type.GetFieldByColumnName(col.Name);

                EmitFieldRead(fieldInfo, il, col, i, local);
            }

            il.Emit(OpCodes.Ldloc_0); // res
            il.Emit(OpCodes.Ret);

            return (ReadDelegate<T>)method.CreateDelegate(typeof(ReadDelegate<T>));
        }

        private static ReadDelegate<T> EmitKvReader(Schema schema, bool keyOnly, DynamicMethod method)
        {
            var type = typeof(T);

            var il = method.GetILGenerator();
            var (keyType, valType, keyField, valField) = GetKeyValTypes();

            var keyMethod = BinaryTupleMethods.GetReadMethodOrNull(keyType);
            var valMethod = BinaryTupleMethods.GetReadMethodOrNull(valType);

            var kvLocal = il.DeclareAndInitLocal(type);
            var keyLocal = keyMethod == null ? il.DeclareAndInitLocal(keyType) : null;
            var valLocal = valMethod == null ? il.DeclareAndInitLocal(valType) : null;

            var columns = schema.Columns;
            var count = keyOnly ? schema.KeyColumnCount : columns.Count;

            for (var i = 0; i < count; i++)
            {
                var col = columns[i];
                FieldInfo? fieldInfo;
                LocalBuilder? local;

                if (i == 0 && keyMethod != null)
                {
                    fieldInfo = keyField;
                    local = kvLocal;
                }
                else if (i == schema.KeyColumnCount && valMethod != null)
                {
                    fieldInfo = valField;
                    local = kvLocal;
                }
                else
                {
                    local = col.IsKey ? keyLocal : valLocal;
                    fieldInfo = local == null
                        ? null
                        : (col.IsKey ? keyType : valType).GetFieldByColumnName(col.Name);
                }

                EmitFieldRead(fieldInfo, il, col, i, local);
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

        private static ReadValuePartDelegate<T> EmitValuePartReader(Schema schema)
        {
            var type = typeof(T);

            if (BinaryTupleMethods.GetReadMethodOrNull(type) != null)
            {
                // Single column to primitive type mapping - return key as is.
                return (ref BinaryTupleReader _, T key) => key;
            }

            var method = new DynamicMethod(
                name: "ReadValuePart" + type,
                returnType: type,
                parameterTypes: new[] { typeof(BinaryTupleReader).MakeByRefType(), type },
                m: typeof(IIgnite).Module,
                skipVisibility: true);

            if (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(KvPair<,>))
            {
                return EmitKvValuePartReader(schema, method);
            }

            var il = method.GetILGenerator();
            var local = il.DeclareAndInitLocal(type); // T res

            var columns = schema.Columns;

            for (var i = 0; i < columns.Count; i++)
            {
                var col = columns[i];
                var fieldInfo = type.GetFieldByColumnName(col.Name);

                if (col.IsKey)
                {
                    if (fieldInfo != null)
                    {
                        il.Emit(type.IsValueType ? OpCodes.Ldloca_S : OpCodes.Ldloc, local); // res
                        il.Emit(OpCodes.Ldarg_1); // key
                        il.Emit(OpCodes.Ldfld, fieldInfo);
                        il.Emit(OpCodes.Stfld, fieldInfo);
                    }

                    continue;
                }

                EmitFieldRead(fieldInfo, il, col, i - schema.KeyColumnCount, local);
            }

            il.Emit(OpCodes.Ldloc_0); // res
            il.Emit(OpCodes.Ret);

            return (ReadValuePartDelegate<T>)method.CreateDelegate(typeof(ReadValuePartDelegate<T>));
        }

        private static ReadValuePartDelegate<T> EmitKvValuePartReader(Schema schema, DynamicMethod method)
        {
            var type = typeof(T);
            var (_, valType, _, valField) = GetKeyValTypes();

            var il = method.GetILGenerator();
            var kvLocal = il.DeclareAndInitLocal(type);

            var valReadMethod = BinaryTupleMethods.GetReadMethodOrNull(valType);

            if (valReadMethod != null)
            {
                // Single-value mapping.
                if (schema.Columns.Count == schema.KeyColumnCount)
                {
                    // No value columns.
                    return (ref BinaryTupleReader _, T _) => default!;
                }

                EmitFieldRead(valField, il, schema.Columns[schema.KeyColumnCount], 0, kvLocal);
            }
            else
            {
                var valLocal = il.DeclareAndInitLocal(valType);
                var columns = schema.Columns;

                for (var i = schema.KeyColumnCount; i < columns.Count; i++)
                {
                    var col = columns[i];
                    var fieldInfo = valType.GetFieldByColumnName(col.Name);

                    EmitFieldRead(fieldInfo, il, col, i - schema.KeyColumnCount, valLocal);
                }

                // Copy Val to KvPair.
                il.Emit(OpCodes.Ldloca_S, kvLocal);
                il.Emit(OpCodes.Ldloc, valLocal);
                il.Emit(OpCodes.Stfld, valField);
            }

            il.Emit(OpCodes.Ldloc_0); // res
            il.Emit(OpCodes.Ret);

            return (ReadValuePartDelegate<T>)method.CreateDelegate(typeof(ReadValuePartDelegate<T>));
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

            if (col.Type == ClientDataType.Decimal)
            {
                il.Emit(OpCodes.Ldc_I4, col.Scale);
            }

            il.Emit(OpCodes.Call, readMethod);
            il.Emit(OpCodes.Stfld, fieldInfo); // res.field = value
        }

        private static void ValidateFieldType(FieldInfo fieldInfo, Column column)
        {
            var columnType = column.Type.ToType();

            var fieldType = Nullable.GetUnderlyingType(fieldInfo.FieldType) ?? fieldInfo.FieldType;
            fieldType = fieldType.UnwrapEnum();

            if (fieldType != columnType)
            {
                var message = $"Can't map field '{fieldInfo.DeclaringType?.Name}.{fieldInfo.Name}' of type '{fieldInfo.FieldType}' " +
                              $"to column '{column.Name}' of type '{columnType}' - types do not match.";

                throw new IgniteClientException(ErrorGroups.Client.Configuration, message);
            }
        }

        private static void ValidateSingleFieldMappingType(Type type, Column column)
        {
            var columnType = column.Type.ToType();

            if (type != columnType)
            {
                var message = $"Can't map '{type}' to column '{column.Name}' of type '{columnType}' - types do not match.";

                throw new IgniteClientException(ErrorGroups.Client.Configuration, message);
            }
        }

        private static void ValidateMappedCount(int mappedCount, Type type, IEnumerable<Column> columns)
        {
            if (mappedCount > 0)
            {
                return;
            }

            var columnStr = string.Join(", ", columns.Select(x => x.Type + " " + x.Name));

            throw new IgniteClientException(
                ErrorGroups.Client.Configuration,
                $"Can't map '{type}' to columns '{columnStr}'. Matching fields not found.");
        }

        private static (Type KeyType, Type ValType, FieldInfo KeyField, FieldInfo ValField) GetKeyValTypes()
        {
            var type = typeof(T);
            Debug.Assert(
                type.IsGenericType && type.GetGenericTypeDefinition() == typeof(KvPair<,>),
                "type.IsGenericType && type.GetGenericTypeDefinition() == typeof(KvPair<,>)");

            var keyValTypes = type.GetGenericArguments();

            return (keyValTypes[0], keyValTypes[1], type.GetFieldByColumnName("Key")!, type.GetFieldByColumnName("Val")!);
        }
    }
}
