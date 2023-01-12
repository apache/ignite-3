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

namespace Apache.Ignite.Internal.Proto.MsgPack;

using System;
using System.Buffers.Binary;
using Buffers;

/// <summary>
/// MsgPack writer.
/// </summary>
internal readonly ref struct MsgPackWriter // TODO: Convert to extension methods, since position is tracked by PooledArrayBufferWriter?
{
    private const int MaxFixPositiveInt = 127;

    private readonly PooledArrayBufferWriter _writer;

    /// <summary>
    /// Initializes a new instance of the <see cref="MsgPackWriter"/> struct.
    /// </summary>
    /// <param name="writer">Writer.    </param>
    public MsgPackWriter(PooledArrayBufferWriter writer)
    {
        _writer = writer;
    }

    /*
    private const int MinFixNegativeInt = -32;
    private const int MaxFixNegativeInt = -1;
    private const int MinFixStringLength = 0;
    private const int MaxFixStringLength = 31;
    private const int MaxFixMapCount = 15;
    private const int MaxFixArrayCount = 15;
    */

    /// <summary>
    /// Writes an unsigned value to specified memory location and returns number of bytes written.
    /// </summary>
    /// <param name="mem">Memory.</param>
    /// <param name="val">Value.</param>
    /// <returns>Bytes written.</returns>
    public static int WriteUnsigned(Memory<byte> mem, long val)
    {
        unchecked
        {
            var span = mem.Span;

            if (val <= MaxFixPositiveInt)
            {
                span[0] = (byte)val;
                return 1;
            }

            if (val <= byte.MaxValue)
            {
                span[0] = MsgPackCode.UInt8;
                span[1] = (byte)val;

                return 2;
            }

            if (val <= ushort.MaxValue)
            {
                span[0] = MsgPackCode.UInt16;
                BinaryPrimitives.WriteUInt16BigEndian(span[1..], (ushort)val);

                return 3;
            }

            if (val <= uint.MaxValue)
            {
                span[0] = MsgPackCode.UInt32;
                BinaryPrimitives.WriteUInt32BigEndian(span[1..], (uint)val);

                return 5;
            }

            span[0] = MsgPackCode.UInt64;
            BinaryPrimitives.WriteUInt64BigEndian(span[1..], (ulong)val);

            return 9;
        }
    }

    /// <summary>
    /// Writes a <see cref="Guid"/> as UUID (RFC #4122).
    /// <para />
    /// <see cref="Guid"/> uses a mixed-endian format which differs from UUID,
    /// see https://en.wikipedia.org/wiki/Universally_unique_identifier#Encoding.
    /// </summary>
    /// <param name="val">Guid.</param>
    public void Write(Guid val)
    {
        // TODO: GetSpanAndAdvance? GetSpanNoAdvance?
        var span = _writer.GetSpan(18);
        span[0] = MsgPackCode.FixExt16;
        span[1] = (byte)ClientMessagePackType.Uuid;

        UuidSerializer.Write(val, span[2..]);

        _writer.Advance(18);
    }
}
