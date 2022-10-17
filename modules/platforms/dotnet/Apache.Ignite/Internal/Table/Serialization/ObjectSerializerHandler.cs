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
        private readonly ConcurrentDictionary<(int, bool), WriteDelegate<T>> _writers = new();

        private readonly ConcurrentDictionary<(int, bool), ReadDelegate<T>> _readers = new();

        private readonly ConcurrentDictionary<int, ReadValuePartDelegate<T>> _valuePartReaders = new();

        private delegate void WriteDelegate<in TV>(ref BinaryTupleBuilder writer, Span<byte> noValueSet, TV value);

        private delegate TV ReadDelegate<out TV>(ref BinaryTupleReader reader);

        private delegate TV ReadValuePartDelegate<TV>(ref BinaryTupleReader reader, TV key);

        /// <inheritdoc/>
        public T Read(ref MessagePackReader reader, Schema schema, bool keyOnly = false)
        {
            var cacheKey = (schema.Version, keyOnly);

            var readDelegate = _readers.TryGetValue(cacheKey, out var w)
                ? w
                : _readers.GetOrAdd(cacheKey, EmitReader(schema, keyOnly));

            var columnCount = keyOnly ? schema.KeyColumnCount : schema.Columns.Count;

            var binaryTupleReader = new BinaryTupleReader(reader.ReadBytesAsMemory(), columnCount);

            return readDelegate(ref binaryTupleReader);
        }

        /// <inheritdoc/>
        public T ReadValuePart(ref MessagePackReader reader, Schema schema, T key)
        {
            var readDelegate = _valuePartReaders.TryGetValue(schema.Version, out var w)
                ? w
                : _valuePartReaders.GetOrAdd(schema.Version, EmitValuePartReader(schema));

            var binaryTupleReader = new BinaryTupleReader(reader.ReadBytesAsMemory(), schema.ValueColumnCount);

            return readDelegate(ref binaryTupleReader, key);
        }

        /// <inheritdoc/>
        public void Write(ref MessagePackWriter writer, Schema schema, T record, bool keyOnly = false)
        {
            var cacheKey = (schema.Version, keyOnly);

            var writeDelegate = _writers.TryGetValue(cacheKey, out var w)
                ? w
                : _writers.GetOrAdd(cacheKey, EmitWriter(schema, keyOnly));

            var count = keyOnly ? schema.KeyColumnCount : schema.Columns.Count;
            var noValueSet = writer.WriteBitSet(count);
            var tupleBuilder = new BinaryTupleBuilder(count);

            try
            {
                writeDelegate(ref tupleBuilder, noValueSet, record);

                var binaryTupleMemory = tupleBuilder.Build();
                writer.Write(binaryTupleMemory.Span);
            }
            finally
            {
                tupleBuilder.Dispose();
            }
        }

        private static WriteDelegate<T> EmitWriter(Schema schema, bool keyOnly)
        {
            var type = typeof(T);
            var isKvPair = type.IsGenericType && type.GetGenericTypeDefinition() == typeof(KvPair<,>);
            var keyValTypes = isKvPair ? type.GetGenericArguments() : null;

            var method = new DynamicMethod(
                name: "Write" + type,
                returnType: typeof(void),
                parameterTypes: new[] { typeof(BinaryTupleBuilder).MakeByRefType(), typeof(Span<byte>), type },
                m: typeof(IIgnite).Module,
                skipVisibility: true);

            var il = method.GetILGenerator();

            var columns = schema.Columns;
            var count = keyOnly ? schema.KeyColumnCount : columns.Count;

            if (BinaryTupleMethods.GetWriteMethodOrNull(type) is { } directWriteMethod)
            {
                // Single column to primitive type mapping.
                var col = columns[0];
                ValidateSingleFieldMappingType(type, col);

                il.Emit(OpCodes.Ldarg_0); // writer
                il.Emit(OpCodes.Ldarg_2); // value

                if (col.Type == ClientDataType.Decimal)
                {
                    EmitLdcI4(il, col.Scale);
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
                var fieldInfo = keyValTypes == null
                    ? type.GetFieldIgnoreCase(col.Name)
                    : index < schema.KeyColumnCount // KvPair.
                        ? keyValTypes[0].GetFieldIgnoreCase(col.Name)
                        : keyValTypes[1].GetFieldIgnoreCase(col.Name);

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

                    if (keyValTypes != null)
                    {
                        // KvPair.
                        var field = index < schema.KeyColumnCount
                            ? type.GetFieldIgnoreCase("Key")
                            : type.GetFieldIgnoreCase("Val");

                        il.Emit(OpCodes.Ldfld, field!);
                    }

                    il.Emit(OpCodes.Ldfld, fieldInfo);

                    if (col.Type == ClientDataType.Decimal)
                    {
                        EmitLdcI4(il, col.Scale);
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
            var isKvPair = type.IsGenericType && type.GetGenericTypeDefinition() == typeof(KvPair<,>);
            var keyValTypes = isKvPair ? type.GetGenericArguments() : null;

            var method = new DynamicMethod(
                name: "Read" + type,
                returnType: type,
                parameterTypes: new[] { typeof(BinaryTupleReader).MakeByRefType() },
                m: typeof(IIgnite).Module,
                skipVisibility: true);

            var il = method.GetILGenerator();

            if (BinaryTupleMethods.GetReadMethodOrNull(type) is { } readMethod)
            {
                // Single column to primitive type mapping.
                il.Emit(OpCodes.Ldarg_0); // reader
                il.Emit(OpCodes.Ldc_I4_0); // index

                if (schema.Columns[0] is { Type: ClientDataType.Decimal } col)
                {
                    EmitLdcI4(il, col.Scale);
                }

                il.Emit(OpCodes.Call, readMethod);
                il.Emit(OpCodes.Ret);

                return (ReadDelegate<T>)method.CreateDelegate(typeof(ReadDelegate<T>));
            }

            var local = il.DeclareLocal(type);

            il.Emit(OpCodes.Ldtoken, type);
            il.Emit(OpCodes.Call, ReflectionUtils.GetTypeFromHandleMethod);
            il.Emit(OpCodes.Call, ReflectionUtils.GetUninitializedObjectMethod);

            il.Emit(OpCodes.Stloc_0); // T res

            var columns = schema.Columns;
            var count = keyOnly ? schema.KeyColumnCount : columns.Count;

            for (var i = 0; i < count; i++)
            {
                var col = columns[i];
                var fieldInfo = keyValTypes == null
                    ? type.GetFieldIgnoreCase(col.Name)
                    : i < schema.KeyColumnCount
                        ? keyValTypes[0].GetFieldIgnoreCase(col.Name)
                        : keyValTypes[1].GetFieldIgnoreCase(col.Name);

                EmitFieldRead(fieldInfo, il, col, i, local);
            }

            il.Emit(OpCodes.Ldloc_0); // res
            il.Emit(OpCodes.Ret);

            return (ReadDelegate<T>)method.CreateDelegate(typeof(ReadDelegate<T>));
        }

        private static ReadValuePartDelegate<T> EmitValuePartReader(Schema schema)
        {
            var type = typeof(T);
            var isKvPair = type.IsGenericType && type.GetGenericTypeDefinition() == typeof(KvPair<,>);
            var keyValTypes = isKvPair ? type.GetGenericArguments() : null;

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

            var il = method.GetILGenerator();
            var local = il.DeclareLocal(type);

            LocalBuilder? localKey = null;
            LocalBuilder? localVal = null;

            if (keyValTypes != null)
            {
                // TODO: Load all
                localKey = il.DeclareLocal(keyValTypes[0]);
                localVal = il.DeclareLocal(keyValTypes[1]);

                il.Emit(OpCodes.Ldtoken, keyValTypes[0]);
                il.Emit(OpCodes.Call, ReflectionUtils.GetTypeFromHandleMethod);
                il.Emit(OpCodes.Call, ReflectionUtils.GetUninitializedObjectMethod);
                il.Emit(OpCodes.Stloc_1); // T res

                il.Emit(OpCodes.Ldtoken, keyValTypes[1]);
                il.Emit(OpCodes.Call, ReflectionUtils.GetTypeFromHandleMethod);
                il.Emit(OpCodes.Call, ReflectionUtils.GetUninitializedObjectMethod);
                il.Emit(OpCodes.Stloc_2); // T res
            }

            if (type.IsValueType)
            {
                il.Emit(OpCodes.Ldloca_S, local);
                il.Emit(OpCodes.Initobj, type);
            }
            else
            {
                il.Emit(OpCodes.Ldtoken, type);
                il.Emit(OpCodes.Call, ReflectionUtils.GetTypeFromHandleMethod);
                il.Emit(OpCodes.Call, ReflectionUtils.GetUninitializedObjectMethod);
                il.Emit(OpCodes.Stloc_0); // T res
            }

            var columns = schema.Columns;

            for (var i = 0; i < columns.Count; i++)
            {
                var col = columns[i];

                var fieldInfo = keyValTypes == null
                    ? type.GetFieldIgnoreCase(col.Name)
                    : i < schema.KeyColumnCount
                        ? keyValTypes[0].GetFieldIgnoreCase(col.Name)
                        : keyValTypes[1].GetFieldIgnoreCase(col.Name);

                var loc = keyValTypes == null
                    ? local
                    : i < schema.KeyColumnCount
                        ? localKey!
                        : localVal!;

                if (i < schema.KeyColumnCount)
                {
                    // if (fieldInfo != null)
                    // {
                    //     il.Emit(loc.LocalType.IsValueType ? OpCodes.Ldloca_S : OpCodes.Ldloc, loc); // res
                    //     il.Emit(OpCodes.Ldarg_1); // key
                    //     il.Emit(OpCodes.Ldfld, fieldInfo);
                    //     il.Emit(OpCodes.Stfld, fieldInfo);
                    // }
                    continue;
                }

                EmitFieldRead(fieldInfo, il, col, i - schema.KeyColumnCount, loc);
            }

            if (keyValTypes != null)
            {
                il.Emit(OpCodes.Ldloca_S, local);
                il.Emit(OpCodes.Ldloc, localKey!);
                il.Emit(OpCodes.Stfld, type.GetFieldIgnoreCase("Key")!);

                il.Emit(OpCodes.Ldloca_S, local);
                il.Emit(OpCodes.Ldloc, localVal!);
                il.Emit(OpCodes.Stfld, type.GetFieldIgnoreCase("Val")!);
            }

            il.Emit(OpCodes.Ldloc_0); // res
            il.Emit(OpCodes.Ret);

            return (ReadValuePartDelegate<T>)method.CreateDelegate(typeof(ReadValuePartDelegate<T>));
        }

        private static void EmitFieldRead(FieldInfo? fieldInfo, ILGenerator il, Column col, int elemIdx, LocalBuilder local)
        {
            if (fieldInfo == null)
            {
                return;
            }

            ValidateFieldType(fieldInfo, col);

            il.Emit(local.LocalType.IsValueType ? OpCodes.Ldloca_S : OpCodes.Ldloc, local); // res
            il.Emit(OpCodes.Ldarg_0); // reader
            EmitLdcI4(il, elemIdx); // index

            if (col.Type == ClientDataType.Decimal)
            {
                EmitLdcI4(il, col.Scale);
            }

            var readMethod = BinaryTupleMethods.GetReadMethod(fieldInfo.FieldType);
            il.Emit(OpCodes.Call, readMethod);
            il.Emit(OpCodes.Stfld, fieldInfo); // res.field = value
        }

        private static void EmitLdcI4(ILGenerator il, int val)
        {
            switch (val)
            {
                case 0:
                    il.Emit(OpCodes.Ldc_I4_0);
                    break;

                case 1:
                    il.Emit(OpCodes.Ldc_I4_1);
                    break;

                case 2:
                    il.Emit(OpCodes.Ldc_I4_2);
                    break;

                case 3:
                    il.Emit(OpCodes.Ldc_I4_3);
                    break;

                case 4:
                    il.Emit(OpCodes.Ldc_I4_4);
                    break;

                case 5:
                    il.Emit(OpCodes.Ldc_I4_5);
                    break;

                case 6:
                    il.Emit(OpCodes.Ldc_I4_6);
                    break;

                case 7:
                    il.Emit(OpCodes.Ldc_I4_7);
                    break;

                case 8:
                    il.Emit(OpCodes.Ldc_I4_8);
                    break;

                default:
                    il.Emit(OpCodes.Ldc_I4, val);
                    break;
            }
        }

        private static void ValidateFieldType(FieldInfo fieldInfo, Column column)
        {
            var columnType = column.Type.ToType();
            var fieldType = fieldInfo.FieldType;

            if (fieldType != columnType)
            {
                var message = $"Can't map field '{fieldInfo.DeclaringType?.Name}.{fieldInfo.Name}' of type '{fieldType}' " +
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
    }
}
