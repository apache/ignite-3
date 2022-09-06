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

        private static readonly MethodInfo AppendNull = typeof(BinaryTupleBuilder).GetMethod("AppendNull")!;
        private static readonly MethodInfo AppendByte = typeof(BinaryTupleBuilder).GetMethod("AppendByte")!;
        private static readonly MethodInfo AppendShort = typeof(BinaryTupleBuilder).GetMethod("AppendShort")!;
        private static readonly MethodInfo AppendInt = typeof(BinaryTupleBuilder).GetMethod("AppendInt")!;
        private static readonly MethodInfo AppendLong = typeof(BinaryTupleBuilder).GetMethod("AppendLong")!;
        private static readonly MethodInfo AppendFloat = typeof(BinaryTupleBuilder).GetMethod("AppendFloat")!;
        private static readonly MethodInfo AppendGuid = typeof(BinaryTupleBuilder).GetMethod("AppendGuid")!;
        private static readonly MethodInfo AppendString = typeof(BinaryTupleBuilder).GetMethod("AppendString")!;

        // TODO: Support all types (IGNITE-15431).
        private static readonly IReadOnlyDictionary<Type, MethodInfo> WriteMethods = new Dictionary<Type, MethodInfo>
        {
            { typeof(string), AppendString },
            { typeof(sbyte), AppendByte },
            { typeof(short), AppendShort },
            { typeof(int), AppendInt },
            { typeof(long), AppendLong },
            { typeof(float), AppendFloat },
            { typeof(Guid), AppendGuid }
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
    }
}
