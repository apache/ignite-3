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
    using System.Reflection;
    using System.Reflection.Emit;
    using System.Runtime.Serialization;
    using Buffers;
    using MessagePack;
    using Proto;

    /// <summary>
    /// Object serializer handler.
    /// </summary>
    /// <typeparam name="T">Object type.</typeparam>
    internal class ObjectSerializerHandler<T> : IRecordSerializerHandler<T>
        where T : class
    {
        private readonly ConcurrentDictionary<int, WriteDelegate<T>> _writers = new();

#pragma warning disable CA1823
        private readonly ConcurrentDictionary<int, ReadDelegate<T>> _readers = new();
#pragma warning restore CA1823

        private delegate void WriteDelegate<in TV>(ref MessagePackWriter writer, TV value);

        private delegate TV ReadDelegate<out TV>(ref MessagePackReader reader);

        /// <inheritdoc/>
        public T Read(ref MessagePackReader reader, Schema schema, bool keyOnly = false)
        {
            var readDelegate = _readers.TryGetValue(schema.Version, out var w)
                ? w
                : _readers.GetOrAdd(schema.Version, EmitReader(schema, keyOnly));

            return readDelegate(ref reader);
        }

        /// <inheritdoc/>
        public T ReadValuePart(PooledBuffer buf, Schema schema, T key)
        {
            // TODO: Emit code for efficient serialization (IGNITE-16341).
            // Skip schema version.
            var r = buf.GetReader();
            r.Skip();

            var columns = schema.Columns;
            var type = typeof(T);
            var res = (T) FormatterServices.GetUninitializedObject(type);

            for (var i = 0; i < columns.Count; i++)
            {
                var col = columns[i];
                var prop = type.GetFieldIgnoreCase(col.Name);

                if (i < schema.KeyColumnCount)
                {
                    if (prop != null)
                    {
                        prop.SetValue(res, prop.GetValue(key));
                    }
                }
                else
                {
                    if (r.TryReadNoValue())
                    {
                        continue;
                    }

                    if (prop != null)
                    {
                        prop.SetValue(res, r.ReadObject(col.Type));
                    }
                    else
                    {
                        r.Skip();
                    }
                }
            }

            return res;
        }

        /// <inheritdoc/>
        public void Write(ref MessagePackWriter writer, Schema schema, T record, bool keyOnly = false)
        {
            var writeDelegate = _writers.TryGetValue(schema.Version, out var w)
                ? w
                : _writers.GetOrAdd(schema.Version, EmitWriter(schema, keyOnly));

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
                    // writer.WriteNoValue();
                    il.Emit(OpCodes.Ldarg_0); // writer
                    il.Emit(OpCodes.Call, MessagePackMethods.WriteNoValue);
                }
                else
                {
                    // writer.WriteObject(prop.GetValue(record));
                    il.Emit(OpCodes.Ldarg_0); // writer
                    il.Emit(OpCodes.Ldarg_1); // record
                    il.Emit(OpCodes.Ldfld, fieldInfo);

                    var writeMethod = MessagePackMethods.GetWriteMethod(fieldInfo.FieldType);

                    if (fieldInfo.FieldType.IsValueType && writeMethod == MessagePackMethods.WriteObject)
                    {
                        il.Emit(OpCodes.Box, fieldInfo.FieldType);
                    }

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

            var columns = schema.Columns;
            var count = keyOnly ? schema.KeyColumnCount : columns.Count;

            // var res = (T) FormatterServices.GetUninitializedObject(type);
            il.Emit(OpCodes.Ldtoken, type);
            il.Emit(OpCodes.Call, ReflectionUtils.GetTypeFromHandleMethod);
            il.Emit(OpCodes.Call, ReflectionUtils.GetUninitializedObjectMethod);

            il.Emit(OpCodes.Stloc_0);

            for (var i = 0; i < columns.Count; i++)
            {
                var col = columns[i];
                var prop = type.GetFieldIgnoreCase(col.Name);

                il.Emit(OpCodes.Ldarg_0); // reader
                il.Emit(OpCodes.Call, MessagePackMethods.ReadNoValue);

                Label noValueLabel = il.DefineLabel();
                il.Emit(OpCodes.Brfalse_S, noValueLabel);

                // TODO: Read into prop here.
                il.MarkLabel(noValueLabel);
            }

            // for (var index = 0; index < count; index++)
            // {
            //     var col = columns[index];
            //     var fieldInfo = type.GetFieldIgnoreCase(col.Name);
            //
            //     if (fieldInfo == null)
            //     {
            //         // writer.WriteNoValue();
            //         il.Emit(OpCodes.Ldarg_0); // writer
            //         il.Emit(OpCodes.Call, MessagePackMethods.WriteNoValue);
            //     }
            //     else
            //     {
            //         // writer.WriteObject(prop.GetValue(record));
            //         il.Emit(OpCodes.Ldarg_0); // writer
            //         il.Emit(OpCodes.Ldarg_1); // record
            //         il.Emit(OpCodes.Ldfld, fieldInfo);
            //
            //         var writeMethod = MessagePackMethods.GetWriteMethod(fieldInfo.FieldType);
            //
            //         if (fieldInfo.FieldType.IsValueType && writeMethod == MessagePackMethods.WriteObject)
            //         {
            //             il.Emit(OpCodes.Box, fieldInfo.FieldType);
            //         }
            //
            //         il.Emit(OpCodes.Call, writeMethod);
            //     }
            // }
            il.Emit(OpCodes.Ldloc_0);
            il.Emit(OpCodes.Ret);

            return (ReadDelegate<T>)method.CreateDelegate(typeof(ReadDelegate<T>));
        }
    }
}
