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
    using System.Collections.Concurrent;
    using System.Diagnostics;
    using System.Diagnostics.CodeAnalysis;
    using System.Reflection;
    using System.Runtime.CompilerServices;

    /// <summary>
    /// Wrapper for the standard <see cref="ArrayPool{T}.Shared"/> with safety checks in debug mode.
    /// </summary>
    internal static class ByteArrayPool
    {
#if DEBUG
        /// <summary>
        /// Gets the currently rented arrays.
        /// </summary>
        public static readonly ConcurrentDictionary<byte[], MethodBase> CurrentlyRentedArrays = new();

        /// <summary>
        /// Track pooled arrays in debug mode to detect double-return - the most dangerous scenario which can cause application-wide
        /// memory corruption, when the same array is returned from the pool twice and used concurrently.
        ///
        /// In the future there should be some built-in ways to detect this:
        /// https://github.com/dotnet/runtime/issues/7532.
        /// </summary>
        private static readonly System.Runtime.CompilerServices.ConditionalWeakTable<byte[], object?> ReturnedArrays = new();
#endif

        /// <summary>
        /// Retrieves a buffer that is at least the requested length.
        /// </summary>
        /// <param name="minimumLength">The minimum length of the array.</param>
        /// <returns>An byte array that is at least <paramref name="minimumLength" /> in length.</returns>
        [UnconditionalSuppressMessage("Trimming", "IL2026", Justification = "Debug mode, guarded.")]
        public static byte[] Rent(int minimumLength)
        {
            var bytes = ArrayPool<byte>.Shared.Rent(minimumLength);

#if DEBUG
            if (RuntimeFeature.IsDynamicCodeSupported)
            {
                var stackTrace = new StackTrace();
                var frame = stackTrace.GetFrame(1);

                CurrentlyRentedArrays.TryAdd(bytes, frame!.GetMethod()!);
                ReturnedArrays.Remove(bytes);
            }
#endif

            return bytes;
        }

        /// <summary>
        /// Returns an array to the pool that was previously obtained using the <see cref="Rent" /> method.
        /// </summary>
        /// <param name="array">A buffer to return to the pool that was previously obtained using the <see cref="Rent" /> method.</param>
        public static void Return(byte[] array)
        {
#if DEBUG
            if (RuntimeFeature.IsDynamicCodeSupported)
            {
                CurrentlyRentedArrays.TryRemove(array, out _);

                // Will throw when key exists.
                ReturnedArrays.Add(array, null);
            }
#endif

            ArrayPool<byte>.Shared.Return(array);
        }
    }
}
