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

namespace Apache.Ignite.Internal.Proto.BinaryTuple
{
    using System;
    using System.Diagnostics;
    using System.Numerics;
    using System.Runtime.InteropServices;

    /// <summary>
    /// Common binary tuple constants and utils.
    /// </summary>
    internal static class BinaryTupleCommon
    {
        /// <summary>
        /// Size of a tuple header, in bytes.
        /// </summary>
        public const int HeaderSize = 1;

        /// <summary>
        /// Mask for size of entries in variable-length offset table.
        /// </summary>
        public const int VarsizeMask = 0b011;

        /// <summary>
        /// Empty varlen token.
        /// </summary>
        public const byte VarlenEmptyByte = 0x80;

        /// <summary>
        /// Calculates flags for a given size of variable-length area.
        /// </summary>
        /// <param name="size">Variable-length area size.</param>
        /// <returns>Flags value.</returns>
        public static byte ValueSizeToFlags(long size)
        {
            if (size <= 0xff)
            {
                return 0b00;
            }

            if (size <= 0xffff)
            {
                return 0b01;
            }

            Debug.Assert(size <= int.MaxValue, "size <= int.MaxValue");

            return 0b10;
        }

        /// <summary>
        /// Calculates the size of entry in variable-length offset table for given flags.
        /// </summary>
        /// <param name="flags">Flags.</param>
        /// <returns>Size of entry in variable-length offset table.</returns>
        public static int FlagsToEntrySize(byte flags)
        {
            return 1 << (flags & VarsizeMask);
        }

        /// <summary>
        /// Converts byte to bool.
        /// </summary>
        /// <param name="value">Byte value.</param>
        /// <returns>Bool value.</returns>
        public static bool ByteToBool(sbyte value)
        {
            Debug.Assert(value is 0 or 1, "value is 0 or 1");

            return value != 0;
        }

        /// <summary>
        /// Converts bool to byte.
        /// </summary>
        /// <param name="value">Bool value.</param>
        /// <returns>Byte value.</returns>
        public static sbyte BoolToByte(bool value) => value ? (sbyte) 1 : (sbyte) 0;

        /// <summary>
        /// Converts decimal to unscaled BigInteger.
        /// </summary>
        /// <param name="value">Decimal value.</param>
        /// <param name="maxScale">Maximum scale to use.</param>
        /// <returns>Unscaled BigInteger and scale.</returns>
        public static (BigInteger BigInt, short Scale) DecimalToUnscaledBigInteger(decimal value, int maxScale)
        {
            if (value == decimal.Zero)
            {
                return (BigInteger.Zero, 0);
            }

            Span<int> bits = stackalloc int[4];
            decimal.GetBits(value, bits);

            var valueScale = (bits[3] & 0x00FF0000) >> 16;
            var sign = bits[3] >> 31;

            var bytes = MemoryMarshal.Cast<int, byte>(bits[..3]);
            var unscaled = new BigInteger(bytes, true);

            if (sign < 0)
            {
                unscaled = -unscaled;
            }

            if (valueScale > maxScale)
            {
                unscaled /= BigInteger.Pow(new BigInteger(10), valueScale - maxScale);
                valueScale = maxScale;
            }

            Debug.Assert(valueScale <= short.MaxValue, "valueScale < short.MaxValue");
            Debug.Assert(valueScale >= short.MinValue, "valueScale > short.MinValue");

            return (unscaled, (short)valueScale);
        }
    }
}
