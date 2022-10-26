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

namespace Apache.Ignite.Internal.Proto;

using System.Numerics;

/// <summary>
/// Hash function based on MurmurHash3
/// (https://commons.apache.org/proper/commons-codec/jacoco/org.apache.commons.codec.digest/MurmurHash3.java.html).
/// <para />
/// Ported from org/apache/ignite/internal/util/HashUtils.java.
/// </summary>
internal static class HashUtils
{
    private const ulong C1 = 0x87c37b91114253d5L;
    private const ulong C2 = 0x4cf5ad432745937fL;
    private const int R1 = 31;

    // TODO IGNITE-17969 Partition Awareness - support all key types
    /*private const int R2 = 27;
    private const int R3 = 33;
    private const int M = 5;
    private const int N1 = 0x52dce729;
    private const int N2 = 0x38495ab5;*/

    /// <summary>
    /// Generates 32-bit hash from the integer value.
    /// </summary>
    /// <param name="data">Input data.</param>
    /// <param name="seed">Current hash.</param>
    /// <returns>Resulting hash.</returns>
    public static int Hash32(int data, int seed)
    {
        ulong hash = Hash64(data, seed);

        return (int)(hash ^ (hash >> 32));
    }

    /// <summary>
    /// Generates 32-bit hash from the long value.
    /// </summary>
    /// <param name="data">Input data.</param>
    /// <param name="seed">Current hash.</param>
    /// <returns>Resulting hash.</returns>
    public static int Hash32(long data, int seed)
    {
        ulong hash = Hash64(data, seed);

        return (int)(hash ^ (hash >> 32));
    }

    private static ulong Hash64(int data, long seed)
    {
        return HashInternal((uint)data, (ulong)seed);
    }

    private static ulong Hash64(long data, long seed)
    {
        return HashInternal((ulong)data, (ulong)seed);
    }

    private static ulong HashInternal(uint data, ulong seed)
    {
        ulong h1 = seed;
        ulong h2 = seed;

        ulong k1 = 0;

        k1 ^= data & 0xffffffffUL;
        k1 *= C1;
        k1 = BitOperations.RotateLeft(k1, R1);
        k1 *= C2;
        h1 ^= k1;

        // finalization
        h1 ^= 4;
        h2 ^= 4;

        h1 += h2;
        h2 += h1;

        h1 = Fmix64(h1);
        h2 = Fmix64(h2);

        return h1 + h2;
    }

    private static ulong HashInternal(ulong data, ulong seed)
    {
        ulong h1 = seed;
        ulong h2 = seed;

        ulong k1 = 0;

        k1 ^= data;
        k1 *= C1;
        k1 = BitOperations.RotateLeft(k1, R1);
        k1 *= C2;
        h1 ^= k1;

        // finalization
        h1 ^= 8;
        h2 ^= 8;

        h1 += h2;
        h2 += h1;

        h1 = Fmix64(h1);
        h2 = Fmix64(h2);

        return h1 + h2;
    }

    private static ulong Fmix64(ulong hash)
    {
        hash ^= (hash >> 33);
        hash *= 0xff51afd7ed558ccdL;
        hash ^= (hash >> 33);
        hash *= 0xc4ceb9fe1a85ec53L;
        hash ^= (hash >> 33);
        return hash;
    }
}
