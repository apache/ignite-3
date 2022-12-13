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
    using System.Collections;
    using System.Collections.Generic;
    using System.Numerics;
    using System.Reflection;
    using NodaTime;
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

        /// <summary>
        /// <see cref="BinaryTupleReader.IsNull"/>.
        /// </summary>
        public static readonly MethodInfo IsNull = typeof(BinaryTupleReader).GetMethod(nameof(BinaryTupleReader.IsNull))!;

        private static readonly MethodInfo AppendByte = typeof(BinaryTupleBuilder).GetMethod(nameof(BinaryTupleBuilder.AppendByte))!;
        private static readonly MethodInfo AppendShort = typeof(BinaryTupleBuilder).GetMethod(nameof(BinaryTupleBuilder.AppendShort))!;
        private static readonly MethodInfo AppendInt = typeof(BinaryTupleBuilder).GetMethod(nameof(BinaryTupleBuilder.AppendInt))!;
        private static readonly MethodInfo AppendLong = typeof(BinaryTupleBuilder).GetMethod(nameof(BinaryTupleBuilder.AppendLong))!;
        private static readonly MethodInfo AppendFloat = typeof(BinaryTupleBuilder).GetMethod(nameof(BinaryTupleBuilder.AppendFloat))!;
        private static readonly MethodInfo AppendDouble = typeof(BinaryTupleBuilder).GetMethod(nameof(BinaryTupleBuilder.AppendDouble))!;
        private static readonly MethodInfo AppendGuid = typeof(BinaryTupleBuilder).GetMethod(nameof(BinaryTupleBuilder.AppendGuid))!;
        private static readonly MethodInfo AppendString = typeof(BinaryTupleBuilder).GetMethod(nameof(BinaryTupleBuilder.AppendStringNullable))!;
        private static readonly MethodInfo AppendDate = typeof(BinaryTupleBuilder).GetMethod(nameof(BinaryTupleBuilder.AppendDate))!;
        private static readonly MethodInfo AppendBitmask = typeof(BinaryTupleBuilder).GetMethod(nameof(BinaryTupleBuilder.AppendBitmask))!;
        private static readonly MethodInfo AppendTime = typeof(BinaryTupleBuilder).GetMethod(nameof(BinaryTupleBuilder.AppendTime))!;
        private static readonly MethodInfo AppendDateTime = typeof(BinaryTupleBuilder).GetMethod(nameof(BinaryTupleBuilder.AppendDateTime))!;
        private static readonly MethodInfo AppendTimestamp = typeof(BinaryTupleBuilder).GetMethod(nameof(BinaryTupleBuilder.AppendTimestamp))!;
        private static readonly MethodInfo AppendDecimal = typeof(BinaryTupleBuilder).GetMethod(nameof(BinaryTupleBuilder.AppendDecimal))!;
        private static readonly MethodInfo AppendNumber = typeof(BinaryTupleBuilder).GetMethod(nameof(BinaryTupleBuilder.AppendNumber))!;
        private static readonly MethodInfo AppendBytes =
            typeof(BinaryTupleBuilder).GetMethod(nameof(BinaryTupleBuilder.AppendBytes), new[] { typeof(byte[]) })!;

        private static readonly MethodInfo GetByte = typeof(BinaryTupleReader).GetMethod(nameof(BinaryTupleReader.GetByte))!;
        private static readonly MethodInfo GetByteAsBool = typeof(BinaryTupleReader).GetMethod(nameof(BinaryTupleReader.GetByteAsBool))!;
        private static readonly MethodInfo GetShort = typeof(BinaryTupleReader).GetMethod(nameof(BinaryTupleReader.GetShort))!;
        private static readonly MethodInfo GetInt = typeof(BinaryTupleReader).GetMethod(nameof(BinaryTupleReader.GetInt))!;
        private static readonly MethodInfo GetLong = typeof(BinaryTupleReader).GetMethod(nameof(BinaryTupleReader.GetLong))!;
        private static readonly MethodInfo GetFloat = typeof(BinaryTupleReader).GetMethod(nameof(BinaryTupleReader.GetFloat))!;
        private static readonly MethodInfo GetDouble = typeof(BinaryTupleReader).GetMethod(nameof(BinaryTupleReader.GetDouble))!;
        private static readonly MethodInfo GetGuid = typeof(BinaryTupleReader).GetMethod(nameof(BinaryTupleReader.GetGuid))!;
        private static readonly MethodInfo GetString = typeof(BinaryTupleReader).GetMethod(nameof(BinaryTupleReader.GetStringNullable))!;
        private static readonly MethodInfo GetDate = typeof(BinaryTupleReader).GetMethod(nameof(BinaryTupleReader.GetDate))!;
        private static readonly MethodInfo GetBitmask = typeof(BinaryTupleReader).GetMethod(nameof(BinaryTupleReader.GetBitmask))!;
        private static readonly MethodInfo GetTime = typeof(BinaryTupleReader).GetMethod(nameof(BinaryTupleReader.GetTime))!;
        private static readonly MethodInfo GetDateTime = typeof(BinaryTupleReader).GetMethod(nameof(BinaryTupleReader.GetDateTime))!;
        private static readonly MethodInfo GetTimestamp = typeof(BinaryTupleReader).GetMethod(nameof(BinaryTupleReader.GetTimestamp))!;
        private static readonly MethodInfo GetDecimal = typeof(BinaryTupleReader).GetMethod(nameof(BinaryTupleReader.GetDecimal))!;
        private static readonly MethodInfo GetNumber = typeof(BinaryTupleReader).GetMethod(nameof(BinaryTupleReader.GetNumber))!;
        private static readonly MethodInfo GetBytes = typeof(BinaryTupleReader).GetMethod(nameof(BinaryTupleReader.GetBytes))!;

        private static readonly IReadOnlyDictionary<Type, MethodInfo> WriteMethods = new Dictionary<Type, MethodInfo>
        {
            { typeof(string), AppendString },
            { typeof(sbyte), AppendByte },
            { typeof(short), AppendShort },
            { typeof(int), AppendInt },
            { typeof(long), AppendLong },
            { typeof(float), AppendFloat },
            { typeof(double), AppendDouble },
            { typeof(Guid), AppendGuid },
            { typeof(LocalDate), AppendDate },
            { typeof(BitArray), AppendBitmask },
            { typeof(LocalTime), AppendTime },
            { typeof(LocalDateTime), AppendDateTime },
            { typeof(Instant), AppendTimestamp },
            { typeof(byte[]), AppendBytes },
            { typeof(decimal), AppendDecimal },
            { typeof(BigInteger), AppendNumber }
        };

        private static readonly IReadOnlyDictionary<Type, MethodInfo> ReadMethods = new Dictionary<Type, MethodInfo>
        {
            { typeof(string), GetString },
            { typeof(sbyte), GetByte },
            { typeof(bool), GetByteAsBool },
            { typeof(short), GetShort },
            { typeof(int), GetInt },
            { typeof(long), GetLong },
            { typeof(float), GetFloat },
            { typeof(double), GetDouble },
            { typeof(Guid), GetGuid },
            { typeof(LocalDate), GetDate },
            { typeof(BitArray), GetBitmask },
            { typeof(LocalTime), GetTime },
            { typeof(LocalDateTime), GetDateTime },
            { typeof(Instant), GetTimestamp },
            { typeof(decimal), GetDecimal },
            { typeof(BigInteger), GetNumber },
            { typeof(byte[]), GetBytes }
        };

        /// <summary>
        /// Gets the write method.
        /// </summary>
        /// <param name="valueType">Type of the value to write.</param>
        /// <returns>Write method for the specified value type.</returns>
        public static MethodInfo GetWriteMethod(Type valueType) =>
            WriteMethods.TryGetValue(valueType, out var method) ? method : throw GetUnsupportedTypeException(valueType);

        /// <summary>
        /// Gets the write method.
        /// </summary>
        /// <param name="valueType">Type of the value to write.</param>
        /// <returns>Write method for the specified value type.</returns>
        public static MethodInfo? GetWriteMethodOrNull(Type valueType) => WriteMethods.GetValueOrDefault(valueType);

        /// <summary>
        /// Gets the read method.
        /// </summary>
        /// <param name="valueType">Type of the value to read.</param>
        /// <returns>Read method for the specified value type.</returns>
        public static MethodInfo GetReadMethod(Type valueType) =>
            ReadMethods.TryGetValue(valueType, out var method) ? method : throw GetUnsupportedTypeException(valueType);

        /// <summary>
        /// Gets the read method.
        /// </summary>
        /// <param name="valueType">Type of the value to read.</param>
        /// <returns>Read method for the specified value type.</returns>
        public static MethodInfo? GetReadMethodOrNull(Type valueType) => ReadMethods.GetValueOrDefault(valueType);

        private static IgniteClientException GetUnsupportedTypeException(Type valueType) =>
            new(ErrorGroups.Client.Configuration, "Unsupported type: " + valueType);
    }
}
