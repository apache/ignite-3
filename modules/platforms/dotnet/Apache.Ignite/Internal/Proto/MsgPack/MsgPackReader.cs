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
using System.Diagnostics;
using System.IO;

/// <summary>
/// MsgPack reader.
/// </summary>
internal ref struct MsgPackReader
{
    private readonly ReadOnlySpan<byte> _span;

    private int _pos;

    /// <summary>
    /// Initializes a new instance of the <see cref="MsgPackReader"/> struct.
    /// </summary>
    /// <param name="span">Span to read from.</param>
    public MsgPackReader(ReadOnlySpan<byte> span)
    {
        _span = span;
        _pos = 0;
    }

    /// <summary>
    /// Reads nil if it is the next token.
    /// </summary>
    /// <returns><c>true</c> if the next token was nil; <c>false</c> otherwise.</returns>
    public bool TryReadNil()
    {
        if (_span[_pos] == MsgPackCode.Nil)
        {
            _pos++;
            return true;
        }

        return false;
    }

    /// <summary>
    /// Reads a long value.
    /// </summary>
    /// <returns>The value.</returns>
    /// <exception cref="OverflowException">Thrown when the value exceeds what can be stored in the returned type.</exception>
    public long ReadInt64() =>
        _span[_pos++] switch
        {
            var code and >= MsgPackCode.MinNegativeFixInt and <= MsgPackCode.MaxNegativeFixInt => unchecked((sbyte)code),
            var code and >= MsgPackCode.MinFixInt and <= MsgPackCode.MaxFixInt => code,
            MsgPackCode.UInt8 => _span[_pos++],
            MsgPackCode.Int8 => unchecked((sbyte)_span[_pos++]),
            MsgPackCode.UInt16 => BinaryPrimitives.ReadUInt16BigEndian(GetSpan(2)),
            MsgPackCode.Int16 => BinaryPrimitives.ReadInt16BigEndian(GetSpan(2)),
            MsgPackCode.UInt32 => BinaryPrimitives.ReadUInt32BigEndian(GetSpan(4)),
            MsgPackCode.Int32 => BinaryPrimitives.ReadInt32BigEndian(GetSpan(4)),
            MsgPackCode.UInt64 => checked((long)BinaryPrimitives.ReadUInt64BigEndian(GetSpan(8))),
            MsgPackCode.Int64 => BinaryPrimitives.ReadInt64BigEndian(GetSpan(8)),
            var invalid => throw GetInvalidCodeException(invalid)
        };

    /// <summary>
    /// Reads array header.
    /// </summary>
    /// <returns>Array size.</returns>
    public int ReadArrayHeader()
    {
        var code = _span[_pos++];

        if (MsgPackCode.IsFixArr(code))
        {
            return code & 0x0F;
        }

        switch (code)
        {
            case MsgPackCode.Array16: return BinaryPrimitives.ReadUInt16BigEndian(GetSpan(2));
            case MsgPackCode.Array32: return BinaryPrimitives.ReadInt32BigEndian(GetSpan(4));

            default:
                Debug.Fail("Unexpected array header: " + code);
                return -1;
        }
    }

    private static InvalidDataException GetInvalidCodeException(byte code) => new("Unexpected code: " + code);

    private ReadOnlySpan<byte> GetSpan(int length)
    {
        var span = _span.Slice(_pos, length);

        _pos += length;

        return span;
    }
}
