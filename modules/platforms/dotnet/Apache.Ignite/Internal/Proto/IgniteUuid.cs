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

#pragma warning disable 649 // Field is never assigned.
namespace Apache.Ignite.Internal.Proto
{
    using System;
    using System.Runtime.InteropServices;

    /// <summary>
    /// Ignite UUID implementation combines a global UUID (generated once per node) and a node-local 8-byte id.
    /// </summary>
    [StructLayout(LayoutKind.Sequential)]
    internal struct IgniteUuid : IEquatable<IgniteUuid>
    {
        /// <summary>
        /// High part.
        /// </summary>
        public long Hi;

        /// <summary>
        /// Low part.
        /// </summary>
        public long Low;

        /// <summary>
        /// Local part.
        /// </summary>
        public long Local;

        /// <summary>
        /// Struct size.
        /// </summary>
        public byte Size;

        /// <inheritdoc/>
        public override bool Equals(object? obj)
        {
            return obj is IgniteUuid other && Equals(other);
        }

        /// <inheritdoc/>
        public override int GetHashCode()
        {
            return HashCode.Combine(Hi, Low, Local, Size);
        }

        /// <inheritdoc/>
        public bool Equals(IgniteUuid other)
        {
            return Hi == other.Hi && Low == other.Low && Local == other.Local && Size == other.Size;
        }
    }
}
