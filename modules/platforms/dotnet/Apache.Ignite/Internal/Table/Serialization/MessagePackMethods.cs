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
    using System.Collections.Generic;
    using System.Reflection;
    using System.Runtime.Serialization;
    using MessagePack;
    using Proto;

    /// <summary>
    /// MethodInfos for <see cref="MessagePackWriter"/> and <see cref="MessagePackReader"/>.
    /// </summary>
    internal static class MessagePackMethods
    {
        /// <summary>
        /// No-value writer.
        /// </summary>
        public static readonly MethodInfo WriteNoValue =
            typeof(MessagePackWriterExtensions).GetMethod(nameof(MessagePackWriterExtensions.WriteNoValue))!;

        /// <summary>
        /// No-value reader.
        /// </summary>
        public static readonly MethodInfo ReadNoValue =
            typeof(MessagePackReaderExtensions).GetMethod(nameof(MessagePackReaderExtensions.TryReadNoValue))!;

        /// <summary>
        /// Object (catch all) writer.
        /// </summary>
        public static readonly MethodInfo WriteObject =
            typeof(MessagePackWriterExtensions).GetMethod(nameof(MessagePackWriterExtensions.WriteObject))!;

        /// <summary>
        /// Object (catch all) reader.
        /// </summary>
        public static readonly MethodInfo ReadObject =
            typeof(MessagePackReaderExtensions).GetMethod(nameof(MessagePackReaderExtensions.ReadObject))!;

        private static readonly IReadOnlyDictionary<Type, MethodInfo> WriteMethods = new Dictionary<Type, MethodInfo>
        {
            { typeof(string), GetWriteMethod<string>() },
            { typeof(byte), GetWriteMethod<byte>() },
            { typeof(sbyte), GetWriteMethod<sbyte>() },
            { typeof(short), GetWriteMethod<short>() },
            { typeof(ushort), GetWriteMethod<ushort>() },
            { typeof(int), GetWriteMethod<int>() },
            { typeof(uint), GetWriteMethod<uint>() },
            { typeof(long), GetWriteMethod<long>() },
            { typeof(ulong), GetWriteMethod<ulong>() },
            { typeof(Guid), GetWriteMethod<Guid>() },
        };

        private static readonly IReadOnlyDictionary<Type, MethodInfo> ReadMethods = new Dictionary<Type, MethodInfo>
        {
            { typeof(string), GetReadMethod<string>() },
            { typeof(byte), GetReadMethod<byte>() },
            { typeof(sbyte), GetReadMethod<sbyte>() },
            { typeof(short), GetReadMethod<short>() },
            { typeof(ushort), GetReadMethod<ushort>() },
            { typeof(int), GetReadMethod<int>() },
            { typeof(uint), GetReadMethod<uint>() },
            { typeof(long), GetReadMethod<long>() },
            { typeof(ulong), GetReadMethod<ulong>() },
            { typeof(Guid), GetReadMethod<Guid>() },
        };

        /// <summary>
        /// Gets the write method.
        /// </summary>
        /// <param name="valueType">Type of the value to write.</param>
        /// <returns>Write method for the specified value type.</returns>
        public static MethodInfo GetWriteMethod(Type valueType) =>
            WriteMethods.TryGetValue(valueType, out var method)
                ? method
                : WriteObject;

        /// <summary>
        /// Gets the read method.
        /// </summary>
        /// <param name="valueType">Type of the value to read.</param>
        /// <returns>Read method for the specified value type.</returns>
        public static MethodInfo GetReadMethod(Type valueType) =>
            ReadMethods.TryGetValue(valueType, out var method)
                ? method
                : ReadObject;

        private static MethodInfo GetWriteMethod<TArg>()
        {
            const string methodName = nameof(MessagePackWriter.Write);

            var methodInfo = typeof(MessagePackWriter).GetMethod(methodName, new[] { typeof(TArg) }) ??
                             typeof(MessagePackWriterExtensions).GetMethod(
                                 methodName, new[] { typeof(MessagePackWriter).MakeByRefType(), typeof(TArg) });

            if (methodInfo == null)
            {
                throw new InvalidOperationException($"Method not found: Write({typeof(TArg).Name})");
            }

            return methodInfo;
        }

        private static MethodInfo GetReadMethod<TRes>()
        {
            var methodName = "Read" + typeof(TRes).Name;

            var methodInfo = typeof(MessagePackReader).GetMethod(methodName) ??
                             typeof(MessagePackReaderExtensions).GetMethod(methodName);

            if (methodInfo == null)
            {
                throw new InvalidOperationException($"Method not found: {methodName}");
            }

            return methodInfo;
        }
    }
}
