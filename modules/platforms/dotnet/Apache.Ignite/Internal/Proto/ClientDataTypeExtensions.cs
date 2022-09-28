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

namespace Apache.Ignite.Internal.Proto
{
    using System;
    using System.Collections;
    using System.Numerics;
    using NodaTime;

    /// <summary>
    /// Extension methods for <see cref="ClientDataType"/>.
    /// </summary>
    internal static class ClientDataTypeExtensions
    {
        /// <summary>
        /// Converts client data type to <see cref="Type"/>.
        /// </summary>
        /// <param name="clientDataType">Client data type.</param>
        /// <returns>Corresponding CLR type.</returns>
        public static Type ToType(this ClientDataType clientDataType) => clientDataType switch
        {
            ClientDataType.Int8 => typeof(sbyte),
            ClientDataType.Int16 => typeof(short),
            ClientDataType.Int32 => typeof(int),
            ClientDataType.Int64 => typeof(long),
            ClientDataType.Float => typeof(float),
            ClientDataType.Double => typeof(double),
            ClientDataType.Decimal => typeof(decimal),
            ClientDataType.Uuid => typeof(Guid),
            ClientDataType.String => typeof(string),
            ClientDataType.Bytes => typeof(byte[]),
            ClientDataType.BitMask => typeof(BitArray),
            ClientDataType.Date => typeof(LocalDate),
            ClientDataType.Time => typeof(LocalTime),
            ClientDataType.DateTime => typeof(LocalDateTime),
            ClientDataType.Timestamp => typeof(Instant),
            ClientDataType.Number => typeof(BigInteger),
            _ => throw new ArgumentOutOfRangeException(nameof(clientDataType), clientDataType, null)
        };
    }
}
