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

namespace Apache.Ignite.Internal.Buffers
{
    using System.Buffers;

    /// <summary>
    /// Wrapper for the standard <see cref="ArrayPool{T}.Shared"/> with safety checks in debug mode.
    /// </summary>
    internal static class ByteArrayPool
    {
        /// <summary>
        /// Retrieves a buffer that is at least the requested length.
        /// </summary>
        /// <param name="minimumLength">The minimum length of the array.</param>
        /// <returns>An byte array that is at least <paramref name="minimumLength" /> in length.</returns>
        public static byte[] Rent(int minimumLength) => ArrayPool<byte>.Shared.Rent(minimumLength);

        /// <summary>
        /// Returns an array to the pool that was previously obtained using the <see cref="Rent" /> method.
        /// </summary>
        /// <param name="array">A buffer to return to the pool that was previously obtained using the <see cref="Rent" /> method.</param>
        public static void Return(byte[] array) => ArrayPool<byte>.Shared.Return(array);
    }
}
