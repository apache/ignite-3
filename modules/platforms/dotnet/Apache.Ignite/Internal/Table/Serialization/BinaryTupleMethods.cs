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

        // Use only nullable variants for reference types (string, bitmask). Use separate methods for value types.
        private static readonly MethodInfo AppendByte = typeof(BinaryTupleBuilder).GetMethod(nameof(BinaryTupleBuilder.AppendByte))!;
        private static readonly MethodInfo AppendByteNullable = typeof(BinaryTupleBuilder).GetMethod(nameof(BinaryTupleBuilder.AppendByteNullable))!;
        private static readonly MethodInfo AppendBool = typeof(BinaryTupleBuilder).GetMethod(nameof(BinaryTupleBuilder.AppendBool))!;
        private static readonly MethodInfo AppendBoolNullable = typeof(BinaryTupleBuilder).GetMethod(nameof(BinaryTupleBuilder.AppendBoolNullable))!;
        private static readonly MethodInfo AppendShort = typeof(BinaryTupleBuilder).GetMethod(nameof(BinaryTupleBuilder.AppendShort))!;
        private static readonly MethodInfo AppendShortNullable = typeof(BinaryTupleBuilder).GetMethod(nameof(BinaryTupleBuilder.AppendShortNullable))!;
        private static readonly MethodInfo AppendInt = typeof(BinaryTupleBuilder).GetMethod(nameof(BinaryTupleBuilder.AppendInt))!;
        private static readonly MethodInfo AppendIntNullable = typeof(BinaryTupleBuilder).GetMethod(nameof(BinaryTupleBuilder.AppendIntNullable))!;
        private static readonly MethodInfo AppendLong = typeof(BinaryTupleBuilder).GetMethod(nameof(BinaryTupleBuilder.AppendLong))!;
        private static readonly MethodInfo AppendLongNullable = typeof(BinaryTupleBuilder).GetMethod(nameof(BinaryTupleBuilder.AppendLongNullable))!;
        private static readonly MethodInfo AppendFloat = typeof(BinaryTupleBuilder).GetMethod(nameof(BinaryTupleBuilder.AppendFloat))!;
        private static readonly MethodInfo AppendFloatNullable = typeof(BinaryTupleBuilder).GetMethod(nameof(BinaryTupleBuilder.AppendFloatNullable))!;
        private static readonly MethodInfo AppendDouble = typeof(BinaryTupleBuilder).GetMethod(nameof(BinaryTupleBuilder.AppendDouble))!;
        private static readonly MethodInfo AppendDoubleNullable = typeof(BinaryTupleBuilder).GetMethod(nameof(BinaryTupleBuilder.AppendDoubleNullable))!;
        private static readonly MethodInfo AppendGuid = typeof(BinaryTupleBuilder).GetMethod(nameof(BinaryTupleBuilder.AppendGuid))!;
        private static readonly MethodInfo AppendGuidNullable = typeof(BinaryTupleBuilder).GetMethod(nameof(BinaryTupleBuilder.AppendGuidNullable))!;
        private static readonly MethodInfo AppendString = typeof(BinaryTupleBuilder).GetMethod(nameof(BinaryTupleBuilder.AppendStringNullable))!;
        private static readonly MethodInfo AppendDate = typeof(BinaryTupleBuilder).GetMethod(nameof(BinaryTupleBuilder.AppendDate))!;
        private static readonly MethodInfo AppendDateNullable = typeof(BinaryTupleBuilder).GetMethod(nameof(BinaryTupleBuilder.AppendDateNullable))!;
        private static readonly MethodInfo AppendTime = typeof(BinaryTupleBuilder).GetMethod(nameof(BinaryTupleBuilder.AppendTime))!;
        private static readonly MethodInfo AppendTimeNullable = typeof(BinaryTupleBuilder).GetMethod(nameof(BinaryTupleBuilder.AppendTimeNullable))!;
        private static readonly MethodInfo AppendDateTime = typeof(BinaryTupleBuilder).GetMethod(nameof(BinaryTupleBuilder.AppendDateTime))!;
        private static readonly MethodInfo AppendDateTimeNullable = typeof(BinaryTupleBuilder).GetMethod(nameof(BinaryTupleBuilder.AppendDateTimeNullable))!;
        private static readonly MethodInfo AppendTimestamp = typeof(BinaryTupleBuilder).GetMethod(nameof(BinaryTupleBuilder.AppendTimestamp))!;
        private static readonly MethodInfo AppendTimestampNullable = typeof(BinaryTupleBuilder).GetMethod(nameof(BinaryTupleBuilder.AppendTimestampNullable))!;
        private static readonly MethodInfo AppendDecimal = typeof(BinaryTupleBuilder).GetMethod(nameof(BinaryTupleBuilder.AppendDecimal))!;
        private static readonly MethodInfo AppendDecimalNullable = typeof(BinaryTupleBuilder).GetMethod(nameof(BinaryTupleBuilder.AppendDecimalNullable))!;
        private static readonly MethodInfo AppendBytes =
            typeof(BinaryTupleBuilder).GetMethod(nameof(BinaryTupleBuilder.AppendBytesNullable), new[] { typeof(byte[]) })!;

        private static readonly MethodInfo GetByte = typeof(BinaryTupleReader).GetMethod(nameof(BinaryTupleReader.GetByte))!;
        private static readonly MethodInfo GetByteNullable = typeof(BinaryTupleReader).GetMethod(nameof(BinaryTupleReader.GetByteNullable))!;
        private static readonly MethodInfo GetBool = typeof(BinaryTupleReader).GetMethod(nameof(BinaryTupleReader.GetBool))!;
        private static readonly MethodInfo GetBoolNullable = typeof(BinaryTupleReader).GetMethod(nameof(BinaryTupleReader.GetBoolNullable))!;
        private static readonly MethodInfo GetShort = typeof(BinaryTupleReader).GetMethod(nameof(BinaryTupleReader.GetShort))!;
        private static readonly MethodInfo GetShortNullable = typeof(BinaryTupleReader).GetMethod(nameof(BinaryTupleReader.GetShortNullable))!;
        private static readonly MethodInfo GetInt = typeof(BinaryTupleReader).GetMethod(nameof(BinaryTupleReader.GetInt))!;
        private static readonly MethodInfo GetIntNullable = typeof(BinaryTupleReader).GetMethod(nameof(BinaryTupleReader.GetIntNullable))!;
        private static readonly MethodInfo GetLong = typeof(BinaryTupleReader).GetMethod(nameof(BinaryTupleReader.GetLong))!;
        private static readonly MethodInfo GetLongNullable = typeof(BinaryTupleReader).GetMethod(nameof(BinaryTupleReader.GetLongNullable))!;
        private static readonly MethodInfo GetFloat = typeof(BinaryTupleReader).GetMethod(nameof(BinaryTupleReader.GetFloat))!;
        private static readonly MethodInfo GetFloatNullable = typeof(BinaryTupleReader).GetMethod(nameof(BinaryTupleReader.GetFloatNullable))!;
        private static readonly MethodInfo GetDouble = typeof(BinaryTupleReader).GetMethod(nameof(BinaryTupleReader.GetDouble))!;
        private static readonly MethodInfo GetDoubleNullable = typeof(BinaryTupleReader).GetMethod(nameof(BinaryTupleReader.GetDoubleNullable))!;
        private static readonly MethodInfo GetGuid = typeof(BinaryTupleReader).GetMethod(nameof(BinaryTupleReader.GetGuid))!;
        private static readonly MethodInfo GetGuidNullable = typeof(BinaryTupleReader).GetMethod(nameof(BinaryTupleReader.GetGuidNullable))!;
        private static readonly MethodInfo GetString = typeof(BinaryTupleReader).GetMethod(nameof(BinaryTupleReader.GetStringNullable))!;
        private static readonly MethodInfo GetDate = typeof(BinaryTupleReader).GetMethod(nameof(BinaryTupleReader.GetDate))!;
        private static readonly MethodInfo GetDateNullable = typeof(BinaryTupleReader).GetMethod(nameof(BinaryTupleReader.GetDateNullable))!;
        private static readonly MethodInfo GetTime = typeof(BinaryTupleReader).GetMethod(nameof(BinaryTupleReader.GetTime))!;
        private static readonly MethodInfo GetTimeNullable = typeof(BinaryTupleReader).GetMethod(nameof(BinaryTupleReader.GetTimeNullable))!;
        private static readonly MethodInfo GetDateTime = typeof(BinaryTupleReader).GetMethod(nameof(BinaryTupleReader.GetDateTime))!;
        private static readonly MethodInfo GetDateTimeNullable = typeof(BinaryTupleReader).GetMethod(nameof(BinaryTupleReader.GetDateTimeNullable))!;
        private static readonly MethodInfo GetTimestamp = typeof(BinaryTupleReader).GetMethod(nameof(BinaryTupleReader.GetTimestamp))!;
        private static readonly MethodInfo GetTimestampNullable = typeof(BinaryTupleReader).GetMethod(nameof(BinaryTupleReader.GetTimestampNullable))!;
        private static readonly MethodInfo GetDecimal = typeof(BinaryTupleReader).GetMethod(nameof(BinaryTupleReader.GetDecimal))!;
        private static readonly MethodInfo GetDecimalNullable = typeof(BinaryTupleReader).GetMethod(nameof(BinaryTupleReader.GetDecimalNullable))!;
        private static readonly MethodInfo GetBytes = typeof(BinaryTupleReader).GetMethod(nameof(BinaryTupleReader.GetBytesNullable))!;

        private static readonly IReadOnlyDictionary<Type, MethodInfo> WriteMethods = new Dictionary<Type, MethodInfo>
        {
            { typeof(string), AppendString },
            { typeof(sbyte), AppendByte },
            { typeof(sbyte?), AppendByteNullable },
            { typeof(bool), AppendBool },
            { typeof(bool?), AppendBoolNullable },
            { typeof(short), AppendShort },
            { typeof(short?), AppendShortNullable },
            { typeof(int), AppendInt },
            { typeof(int?), AppendIntNullable },
            { typeof(long), AppendLong },
            { typeof(long?), AppendLongNullable },
            { typeof(float), AppendFloat },
            { typeof(float?), AppendFloatNullable },
            { typeof(double), AppendDouble },
            { typeof(double?), AppendDoubleNullable },
            { typeof(Guid), AppendGuid },
            { typeof(Guid?), AppendGuidNullable },
            { typeof(LocalDate), AppendDate },
            { typeof(LocalDate?), AppendDateNullable },
            { typeof(LocalTime), AppendTime },
            { typeof(LocalTime?), AppendTimeNullable },
            { typeof(LocalDateTime), AppendDateTime },
            { typeof(LocalDateTime?), AppendDateTimeNullable },
            { typeof(Instant), AppendTimestamp },
            { typeof(Instant?), AppendTimestampNullable },
            { typeof(byte[]), AppendBytes },
            { typeof(decimal), AppendDecimal },
            { typeof(decimal?), AppendDecimalNullable },
        };

        private static readonly IReadOnlyDictionary<Type, MethodInfo> ReadMethods = new Dictionary<Type, MethodInfo>
        {
            { typeof(string), GetString },
            { typeof(sbyte), GetByte },
            { typeof(sbyte?), GetByteNullable },
            { typeof(bool), GetBool },
            { typeof(bool?), GetBoolNullable },
            { typeof(short), GetShort },
            { typeof(short?), GetShortNullable },
            { typeof(int), GetInt },
            { typeof(int?), GetIntNullable },
            { typeof(long), GetLong },
            { typeof(long?), GetLongNullable },
            { typeof(float), GetFloat },
            { typeof(float?), GetFloatNullable },
            { typeof(double), GetDouble },
            { typeof(double?), GetDoubleNullable },
            { typeof(Guid), GetGuid },
            { typeof(Guid?), GetGuidNullable },
            { typeof(LocalDate), GetDate },
            { typeof(LocalDate?), GetDateNullable },
            { typeof(LocalTime), GetTime },
            { typeof(LocalTime?), GetTimeNullable },
            { typeof(LocalDateTime), GetDateTime },
            { typeof(LocalDateTime?), GetDateTimeNullable },
            { typeof(Instant), GetTimestamp },
            { typeof(Instant?), GetTimestampNullable },
            { typeof(decimal), GetDecimal },
            { typeof(decimal?), GetDecimalNullable },
            { typeof(byte[]), GetBytes }
        };

        /// <summary>
        /// Gets the write method.
        /// </summary>
        /// <param name="type">Type of the value to write.</param>
        /// <returns>Write method for the specified value type.</returns>
        public static MethodInfo GetWriteMethod(Type type) =>
            WriteMethods.TryGetValue(Unwrap(type), out var method) ? method : throw GetUnsupportedTypeException(type);

        /// <summary>
        /// Gets the write method.
        /// </summary>
        /// <param name="type">Type of the value to write.</param>
        /// <returns>Write method for the specified value type.</returns>
        public static MethodInfo? GetWriteMethodOrNull(Type type) => WriteMethods.GetValueOrDefault(Unwrap(type));

        /// <summary>
        /// Gets the read method.
        /// </summary>
        /// <param name="type">Type of the value to read.</param>
        /// <returns>Read method for the specified value type.</returns>
        public static MethodInfo GetReadMethod(Type type) =>
            ReadMethods.TryGetValue(Unwrap(type), out var method) ? method : throw GetUnsupportedTypeException(type);

        /// <summary>
        /// Gets the read method.
        /// </summary>
        /// <param name="type">Type of the value to read.</param>
        /// <returns>Read method for the specified value type.</returns>
        public static MethodInfo? GetReadMethodOrNull(Type type) => ReadMethods.GetValueOrDefault(Unwrap(type));

        private static IgniteClientException GetUnsupportedTypeException(Type valueType) =>
            new(ErrorGroups.Client.Configuration, "Unsupported type: " + valueType);

        private static Type Unwrap(Type type) => type.UnwrapEnum();
    }
}
