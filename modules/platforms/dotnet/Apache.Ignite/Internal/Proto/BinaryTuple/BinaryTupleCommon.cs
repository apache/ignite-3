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
        /// Calculates the null map size.
        /// </summary>
        /// <param name="numElements">Number of tuple elements.</param>
        /// <returns>Null map size in bytes.</returns>
        public static int NullMapSize(int numElements)
        {
            return (numElements + 7) / 8;
        }

        /// <summary>
        /// Returns offset of the byte that contains null-bit of a given tuple element.
        /// </summary>
        /// <param name="index">Tuple element index.</param>
        /// <returns>Offset of the required byte relative to the tuple start.</returns>
        public static int NullOffset(int index)
        {
            return HeaderSize + index / 8;
        }

        /// <summary>
        /// Returns a null-bit mask corresponding to a given tuple element.
        /// </summary>
        /// <param name="index">Tuple element index.</param>
        /// <returns>Mask to extract the required null-bit.</returns>
        public static byte NullMask(int index)
        {
            return (byte)(1 << (index % 8));
        }

        /// <summary>
        /// Converts decimal to unscaled BigInteger.
        /// </summary>
        /// <param name="value">Decimal value.</param>
        /// <param name="scale">Column scale.</param>
        /// <returns>Unscaled BigInteger according to column scale.</returns>
        public static BigInteger DecimalToUnscaledBigInteger(decimal value, int scale)
        {
            if (value == decimal.Zero)
            {
                return BigInteger.Zero;
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

            if (scale > valueScale)
            {
                unscaled *= BigInteger.Pow(new BigInteger(10), scale - valueScale);
            }
            else if (scale < valueScale)
            {
                unscaled /= BigInteger.Pow(new BigInteger(10), valueScale - scale);
            }

            return unscaled;
        }
    }
}
