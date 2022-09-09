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
    using Proto.BinaryTuple;

    /// <summary>
    /// MethodInfos for <see cref="BinaryTupleBuilder"/> and <see cref="BinaryTupleReader"/>.
    /// </summary>
    internal static class BinaryTupleMethods
    {
        /// <summary>
        /// No-value writer.
        /// </summary>
        public static readonly MethodInfo WriteNoValue =
            typeof(BinaryTupleBuilderExtensions).GetMethod(nameof(BinaryTupleBuilderExtensions.AppendNoValue))!;

        private static readonly MethodInfo AppendByte = typeof(BinaryTupleBuilder).GetMethod(nameof(BinaryTupleBuilder.AppendByte))!;
        private static readonly MethodInfo AppendShort = typeof(BinaryTupleBuilder).GetMethod(nameof(BinaryTupleBuilder.AppendShort))!;
        private static readonly MethodInfo AppendInt = typeof(BinaryTupleBuilder).GetMethod(nameof(BinaryTupleBuilder.AppendInt))!;
        private static readonly MethodInfo AppendLong = typeof(BinaryTupleBuilder).GetMethod(nameof(BinaryTupleBuilder.AppendLong))!;
        private static readonly MethodInfo AppendFloat = typeof(BinaryTupleBuilder).GetMethod(nameof(BinaryTupleBuilder.AppendFloat))!;
        private static readonly MethodInfo AppendDouble = typeof(BinaryTupleBuilder).GetMethod(nameof(BinaryTupleBuilder.AppendDouble))!;
        private static readonly MethodInfo AppendGuid = typeof(BinaryTupleBuilder).GetMethod(nameof(BinaryTupleBuilder.AppendGuid))!;
        private static readonly MethodInfo AppendString = typeof(BinaryTupleBuilder).GetMethod(nameof(BinaryTupleBuilder.AppendStringNullable))!;

        private static readonly MethodInfo GetByte = typeof(BinaryTupleReader).GetMethod(nameof(BinaryTupleReader.GetByte))!;
        private static readonly MethodInfo GetShort = typeof(BinaryTupleReader).GetMethod(nameof(BinaryTupleReader.GetShort))!;
        private static readonly MethodInfo GetInt = typeof(BinaryTupleReader).GetMethod(nameof(BinaryTupleReader.GetInt))!;
        private static readonly MethodInfo GetLong = typeof(BinaryTupleReader).GetMethod(nameof(BinaryTupleReader.GetLong))!;
        private static readonly MethodInfo GetFloat = typeof(BinaryTupleReader).GetMethod(nameof(BinaryTupleReader.GetFloat))!;
        private static readonly MethodInfo GetDouble = typeof(BinaryTupleReader).GetMethod(nameof(BinaryTupleReader.GetDouble))!;
        private static readonly MethodInfo GetGuid = typeof(BinaryTupleReader).GetMethod(nameof(BinaryTupleReader.GetGuid))!;
        private static readonly MethodInfo GetString = typeof(BinaryTupleReader).GetMethod(nameof(BinaryTupleReader.GetStringNullable))!;

        // TODO: Support all types (IGNITE-15431).
        private static readonly IReadOnlyDictionary<Type, MethodInfo> WriteMethods = new Dictionary<Type, MethodInfo>
        {
            { typeof(string), AppendString },
            { typeof(sbyte), AppendByte },
            { typeof(short), AppendShort },
            { typeof(int), AppendInt },
            { typeof(long), AppendLong },
            { typeof(float), AppendFloat },
            { typeof(double), AppendDouble },
            { typeof(Guid), AppendGuid }
        };

        private static readonly IReadOnlyDictionary<Type, MethodInfo> ReadMethods = new Dictionary<Type, MethodInfo>
        {
            { typeof(string), GetString },
            { typeof(sbyte), GetByte },
            { typeof(short), GetShort },
            { typeof(int), GetInt },
            { typeof(long), GetLong },
            { typeof(float), GetFloat },
            { typeof(double), GetDouble },
            { typeof(Guid), GetGuid }
        };

        /// <summary>
        /// Gets the write method.
        /// </summary>
        /// <param name="valueType">Type of the value to write.</param>
        /// <returns>Write method for the specified value type.</returns>
        public static MethodInfo GetWriteMethod(Type valueType) =>
            WriteMethods.TryGetValue(valueType, out var method)
                ? method
                : throw new IgniteClientException("Unsupported type: " + valueType);

        /// <summary>
        /// Gets the read method.
        /// </summary>
        /// <param name="valueType">Type of the value to read.</param>
        /// <returns>Read method for the specified value type.</returns>
        public static MethodInfo GetReadMethod(Type valueType) =>
            ReadMethods.TryGetValue(valueType, out var method)
                ? method
                : throw new IgniteClientException("Unsupported type: " + valueType);
    }
}
