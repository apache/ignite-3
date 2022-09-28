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
        public static (Type Primary, Type? Alternative) ToType(this ClientDataType clientDataType)
        {
            return clientDataType switch
            {
                ClientDataType.Int8 => (typeof(byte), typeof(sbyte)),
                ClientDataType.Int16 => (typeof(short), typeof(ushort)),
                ClientDataType.Int32 => (typeof(int), typeof(uint)),
                ClientDataType.Int64 => (typeof(long), typeof(ulong)),
                ClientDataType.Float => (typeof(float), null),
                ClientDataType.Double => (typeof(double), null),
                ClientDataType.Decimal => (typeof(decimal), null),
                ClientDataType.Uuid => (typeof(Guid), null),
                ClientDataType.String => (typeof(string), null),
                ClientDataType.Bytes => (typeof(byte[]), null),
                ClientDataType.BitMask => (typeof(BitArray), null),
                ClientDataType.Date => (typeof(LocalDate), null),
                ClientDataType.Time => (typeof(LocalTime), null),
                ClientDataType.DateTime => (typeof(LocalDateTime), null),
                ClientDataType.Timestamp => (typeof(Instant), null),
                ClientDataType.Number => (typeof(BigInteger), null),
                _ => throw new ArgumentOutOfRangeException(nameof(clientDataType), clientDataType, null)
            };
        }
    }
}
