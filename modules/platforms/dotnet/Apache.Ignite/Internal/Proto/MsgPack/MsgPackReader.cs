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
using System.IO;

/// <summary>
/// MsgPack reader.
/// </summary>
internal ref struct MsgPackReader
{
    // TODO: Write tests only after we check usages. Do not create and test unnecessary methods.
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
    /// Reads a boolean value.
    /// </summary>
    /// <returns>The value.</returns>
    public bool ReadBoolean() =>
        _span[_pos++] switch
        {
            MsgPackCode.True => true,
            MsgPackCode.False => false,
            var invalid => throw GetInvalidCodeException("bool", invalid)
        };

    /// <summary>
    /// Reads an int value.
    /// </summary>
    /// <returns>The value.</returns>
    public int ReadInt32() =>
        _span[_pos++] switch
        {
            var code and >= MsgPackCode.MinNegativeFixInt and <= MsgPackCode.MaxNegativeFixInt => unchecked((sbyte)code),
            var code and >= MsgPackCode.MinFixInt and <= MsgPackCode.MaxFixInt => code,
            MsgPackCode.UInt8 => _span[_pos++],
            MsgPackCode.Int8 => unchecked((sbyte)_span[_pos++]),
            MsgPackCode.UInt16 => BinaryPrimitives.ReadUInt16BigEndian(GetSpan(2)),
            MsgPackCode.Int16 => BinaryPrimitives.ReadInt16BigEndian(GetSpan(2)),
            MsgPackCode.UInt32 => checked((int)BinaryPrimitives.ReadUInt32BigEndian(GetSpan(4))),
            MsgPackCode.Int32 => BinaryPrimitives.ReadInt32BigEndian(GetSpan(4)),
            var invalid => throw GetInvalidCodeException("int32", invalid)
        };

    /// <summary>
    /// Reads a long value.
    /// </summary>
    /// <returns>The value.</returns>
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
            var invalid => throw GetInvalidCodeException("int64", invalid)
        };

    /// <summary>
    /// Reads array header.
    /// </summary>
    /// <returns>Array size.</returns>
    public int ReadArrayHeader() =>
        _span[_pos++] switch
        {
            var code when MsgPackCode.IsFixArr(code) => code & 0x0F,
            MsgPackCode.Array16 => BinaryPrimitives.ReadUInt16BigEndian(GetSpan(2)),
            MsgPackCode.Array32 => checked((int)BinaryPrimitives.ReadUInt32BigEndian(GetSpan(4))),
            var invalid => throw GetInvalidCodeException("array", invalid)
        };

    /// <summary>
    /// Reads binary header.
    /// </summary>
    /// <returns>Binary size.</returns>
    public int ReadBinaryHeader() =>
        _span[_pos++] switch
        {
            var code when MsgPackCode.IsFixRaw(code) => code & 0x1F,
            MsgPackCode.Bin8 => _span[_pos++],
            MsgPackCode.Bin16 => BinaryPrimitives.ReadUInt16BigEndian(GetSpan(2)),
            MsgPackCode.Bin32 => checked((int)BinaryPrimitives.ReadUInt32BigEndian(GetSpan(4))),
            var invalid => throw GetInvalidCodeException("binary", invalid)
        };

    /// <summary>
    /// Reads bytes.
    /// </summary>
    /// <returns>Span of byte.</returns>
    public ReadOnlySpan<byte> ReadBinary() => GetSpan(ReadBinaryHeader());

    private static InvalidDataException GetInvalidCodeException(string expected, byte code) =>
        new($"Invalid code, expected '{expected}', but got '{code}'");

    private ReadOnlySpan<byte> GetSpan(int length)
    {
        var span = _span.Slice(_pos, length);

        _pos += length;

        return span;
    }
}
