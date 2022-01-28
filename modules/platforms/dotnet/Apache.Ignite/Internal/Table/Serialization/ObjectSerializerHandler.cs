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
    using System.Collections.Concurrent;
    using System.Reflection;
    using System.Reflection.Emit;
    using MessagePack;
    using Proto;

    /// <summary>
    /// Object serializer handler.
    /// </summary>
    /// <typeparam name="T">Object type.</typeparam>
    internal class ObjectSerializerHandler<T> : IRecordSerializerHandler<T>
        where T : class
    {
        private readonly ConcurrentDictionary<(int, bool), WriteDelegate<T>> _writers = new();

        private readonly ConcurrentDictionary<(int, bool), ReadDelegate<T>> _readers = new();

        private readonly ConcurrentDictionary<int, ReadValuePartDelegate<T>> _valuePartReaders = new();

        private delegate void WriteDelegate<in TV>(ref MessagePackWriter writer, TV value);

        private delegate TV ReadDelegate<out TV>(ref MessagePackReader reader);

        private delegate TV ReadValuePartDelegate<TV>(ref MessagePackReader reader, TV key);

        /// <inheritdoc/>
        public T Read(ref MessagePackReader reader, Schema schema, bool keyOnly = false)
        {
            var cacheKey = (schema.Version, keyOnly);

            var readDelegate = _readers.TryGetValue(cacheKey, out var w)
                ? w
                : _readers.GetOrAdd(cacheKey, EmitReader(schema, keyOnly));

            return readDelegate(ref reader);
        }

        /// <inheritdoc/>
        public T ReadValuePart(ref MessagePackReader reader, Schema schema, T key)
        {
            var readDelegate = _valuePartReaders.TryGetValue(schema.Version, out var w)
                ? w
                : _valuePartReaders.GetOrAdd(schema.Version, EmitValuePartReader(schema));

            return readDelegate(ref reader, key);
        }

        /// <inheritdoc/>
        public void Write(ref MessagePackWriter writer, Schema schema, T record, bool keyOnly = false)
        {
            var cacheKey = (schema.Version, keyOnly);

            var writeDelegate = _writers.TryGetValue(cacheKey, out var w)
                ? w
                : _writers.GetOrAdd(cacheKey, EmitWriter(schema, keyOnly));

            writeDelegate(ref writer, record);
        }

        private static WriteDelegate<T> EmitWriter(Schema schema, bool keyOnly)
        {
            var type = typeof(T);

            var method = new DynamicMethod(
                name: "Write" + type.Name,
                returnType: typeof(void),
                parameterTypes: new[] { typeof(MessagePackWriter).MakeByRefType(), type },
                m: typeof(IIgnite).Module,
                skipVisibility: true);

            var il = method.GetILGenerator();

            var columns = schema.Columns;
            var count = keyOnly ? schema.KeyColumnCount : columns.Count;

            for (var index = 0; index < count; index++)
            {
                var col = columns[index];
                var fieldInfo = type.GetFieldIgnoreCase(col.Name);

                if (fieldInfo == null)
                {
                    il.Emit(OpCodes.Ldarg_0); // writer
                    il.Emit(OpCodes.Call, MessagePackMethods.WriteNoValue);
                }
                else
                {
                    ValidateFieldType(fieldInfo, col);
                    il.Emit(OpCodes.Ldarg_0); // writer
                    il.Emit(OpCodes.Ldarg_1); // record
                    il.Emit(OpCodes.Ldfld, fieldInfo);

                    var writeMethod = MessagePackMethods.GetWriteMethod(fieldInfo.FieldType);
                    il.Emit(OpCodes.Call, writeMethod);
                }
            }

            il.Emit(OpCodes.Ret);

            return (WriteDelegate<T>)method.CreateDelegate(typeof(WriteDelegate<T>));
        }

        private static ReadDelegate<T> EmitReader(Schema schema, bool keyOnly)
        {
            var type = typeof(T);

            var method = new DynamicMethod(
                name: "Read" + type.Name,
                returnType: type,
                parameterTypes: new[] { typeof(MessagePackReader).MakeByRefType() },
                m: typeof(IIgnite).Module,
                skipVisibility: true);

            var il = method.GetILGenerator();
            il.DeclareLocal(type);

            il.Emit(OpCodes.Ldtoken, type);
            il.Emit(OpCodes.Call, ReflectionUtils.GetTypeFromHandleMethod);
            il.Emit(OpCodes.Call, ReflectionUtils.GetUninitializedObjectMethod);

            il.Emit(OpCodes.Stloc_0); // T res

            var columns = schema.Columns;
            var count = keyOnly ? schema.KeyColumnCount : columns.Count;

            for (var i = 0; i < count; i++)
            {
                var col = columns[i];
                var fieldInfo = type.GetFieldIgnoreCase(col.Name);

                EmitFieldRead(fieldInfo, il, col);
            }

            il.Emit(OpCodes.Ldloc_0); // res
            il.Emit(OpCodes.Ret);

            return (ReadDelegate<T>)method.CreateDelegate(typeof(ReadDelegate<T>));
        }

        private static ReadValuePartDelegate<T> EmitValuePartReader(Schema schema)
        {
            var type = typeof(T);

            var method = new DynamicMethod(
                name: "ReadValuePart" + type.Name,
                returnType: type,
                parameterTypes: new[] { typeof(MessagePackReader).MakeByRefType(), type },
                m: typeof(IIgnite).Module,
                skipVisibility: true);

            var il = method.GetILGenerator();
            il.DeclareLocal(type);

            il.Emit(OpCodes.Ldtoken, type);
            il.Emit(OpCodes.Call, ReflectionUtils.GetTypeFromHandleMethod);
            il.Emit(OpCodes.Call, ReflectionUtils.GetUninitializedObjectMethod);

            il.Emit(OpCodes.Stloc_0); // T res

            var columns = schema.Columns;

            for (var i = 0; i < columns.Count; i++)
            {
                var col = columns[i];
                var fieldInfo = type.GetFieldIgnoreCase(col.Name);

                if (i < schema.KeyColumnCount)
                {
                    if (fieldInfo != null)
                    {
                        il.Emit(OpCodes.Ldloc_0); // res
                        il.Emit(OpCodes.Ldarg_1); // key
                        il.Emit(OpCodes.Ldfld, fieldInfo);
                        il.Emit(OpCodes.Stfld, fieldInfo);
                    }

                    continue;
                }

                EmitFieldRead(fieldInfo, il, col);
            }

            il.Emit(OpCodes.Ldloc_0); // res
            il.Emit(OpCodes.Ret);

            return (ReadValuePartDelegate<T>)method.CreateDelegate(typeof(ReadValuePartDelegate<T>));
        }

        private static void EmitFieldRead(FieldInfo? fieldInfo, ILGenerator il, Column col)
        {
            if (fieldInfo == null)
            {
                il.Emit(OpCodes.Ldarg_0); // reader
                il.Emit(OpCodes.Call, MessagePackMethods.Skip);
            }
            else
            {
                ValidateFieldType(fieldInfo, col);
                il.Emit(OpCodes.Ldarg_0); // reader
                il.Emit(OpCodes.Call, MessagePackMethods.TryReadNoValue);

                Label noValueLabel = il.DefineLabel();
                il.Emit(OpCodes.Brtrue_S, noValueLabel);

                var readMethod = MessagePackMethods.GetReadMethod(fieldInfo.FieldType);

                il.Emit(OpCodes.Ldloc_0); // res
                il.Emit(OpCodes.Ldarg_0); // reader

                il.Emit(OpCodes.Call, readMethod);
                il.Emit(OpCodes.Stfld, fieldInfo); // res.field = value

                il.MarkLabel(noValueLabel);
            }
        }

        private static void ValidateFieldType(FieldInfo fieldInfo, Column column)
        {
            var (columnTypePrimary, columnTypeAlternative) = column.Type.ToType();
            var fieldType = fieldInfo.FieldType;

            if (fieldType != columnTypePrimary && fieldType != columnTypeAlternative)
            {
                throw new IgniteClientException(
                    $"Can't map field '{fieldInfo.DeclaringType?.Name}.{fieldInfo.Name}' of type '{fieldType}' " +
                    $"to column '{column.Name}' of type '{columnTypePrimary}' - types do not match.");
            }
        }
    }
}
