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
using BinaryTuple;
using Ignite.Sql;
using Marshalling;
using NodaTime;

/// <summary>
/// MsgPack reader.
/// </summary>
// ReSharper disable PatternIsRedundant
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
    /// Gets the number of consumed bytes.
    /// </summary>
    public int Consumed => _pos;

    /// <summary>
    /// Gets a value indicating whether the end of the underlying buffer is reached.
    /// </summary>
    public bool End => _pos >= _span.Length;

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
    /// Reads a nullable boolean value.
    /// </summary>
    /// <returns>The value.</returns>
    public bool ReadBooleanNullable() => TryReadNil() ? default : ReadBoolean();

    /// <summary>
    /// Reads a short value.
    /// </summary>
    /// <returns>The value.</returns>
    public short ReadInt16() =>
        _span[_pos++] switch
        {
            var code and >= MsgPackCode.MinNegativeFixInt and <= MsgPackCode.MaxNegativeFixInt => unchecked((sbyte)code),
            var code and >= MsgPackCode.MinFixInt and <= MsgPackCode.MaxFixInt => code,
            MsgPackCode.UInt8 => _span[_pos++],
            MsgPackCode.Int8 => unchecked((sbyte)_span[_pos++]),
            MsgPackCode.UInt16 => checked((short)BinaryPrimitives.ReadUInt16BigEndian(GetSpan(2))),
            MsgPackCode.Int16 => BinaryPrimitives.ReadInt16BigEndian(GetSpan(2)),
            var invalid => throw GetInvalidCodeException("int32", invalid)
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
    /// Reads int or null.
    /// </summary>
    /// <returns>Nullable int.</returns>
    public int? ReadInt32Nullable() => TryReadNil() ? null : ReadInt32();

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
    /// Reads an int value if it is the next token.
    /// </summary>
    /// <param name="res">result.</param>
    /// <returns><c>true</c> if could read an integer value; <c>false</c> otherwise.</returns>
    public bool TryReadInt(out int res)
    {
        if (MsgPackCode.IsInt32(_span[_pos]))
        {
            res = ReadInt32();
            return true;
        }

        res = default;
        return false;
    }

    /// <summary>
    /// Reads string header.
    /// </summary>
    /// <returns>String length in bytes.</returns>
    public int ReadStringHeader() =>
        _span[_pos++] switch
        {
            var code when MsgPackCode.IsFixStr(code) => code & 0x1F,
            MsgPackCode.Str8 => _span[_pos++],
            MsgPackCode.Str16 => BinaryPrimitives.ReadUInt16BigEndian(GetSpan(2)),
            MsgPackCode.Str32 => checked((int)BinaryPrimitives.ReadUInt32BigEndian(GetSpan(4))),
            var invalid => throw GetInvalidCodeException("string", invalid)
        };

    /// <summary>
    /// Reads string value.
    /// </summary>
    /// <returns>String.</returns>
    public string ReadString() => ProtoCommon.StringEncoding.GetString(GetSpan(ReadStringHeader()));

    /// <summary>
    /// Reads string value.
    /// </summary>
    /// <returns>String.</returns>
    public string? ReadStringNullable() => TryReadNil() ? null : ReadString();

    /// <summary>
    /// Reads binary header.
    /// </summary>
    /// <returns>Binary size.</returns>
    public int ReadBinaryHeader() =>
        _span[_pos++] switch
        {
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

    /// <summary>
    /// Reads GUID value.
    /// </summary>
    /// <returns>Guid.</returns>
    public Guid ReadGuid()
    {
        CheckCode(nameof(MsgPackCode.FixExt16), MsgPackCode.FixExt16);
        CheckCode(nameof(ClientMessagePackType.Uuid), (byte)ClientMessagePackType.Uuid);

        return UuidSerializer.Read(GetSpan(16));
    }

    /// <summary>
    /// Reads Instant value.
    /// </summary>
    /// <returns>Instant.</returns>
    public Instant? ReadInstantNullable() => TryReadNil()
        ? null
        : Instant.FromUnixTimeSeconds(ReadInt64()).PlusNanoseconds(ReadInt32());

    /// <summary>
    /// Skips a value.
    /// </summary>
    /// <param name="count">Count of elements to skip.</param>
    public void Skip(int count = 1)
    {
        while (count > 0)
        {
            count--;

            var code = _span[_pos++];

            if (MsgPackCode.IsPosFixInt(code) || MsgPackCode.IsNegFixInt(code))
            {
                continue;
            }

            if (MsgPackCode.IsFixMap(code))
            {
                int mapLen = code & 0x0f;
                count += mapLen * 2;
                continue;
            }

            if (MsgPackCode.IsFixArr(code))
            {
                int arrayLen = code & 0x0f;
                count += arrayLen;
                continue;
            }

            if (MsgPackCode.IsFixStr(code))
            {
                int strLen = code & 0x1f;
                _pos += strLen;
                continue;
            }

            switch (code)
            {
                case MsgPackCode.True:
                case MsgPackCode.False:
                case MsgPackCode.Nil:
                    break;

                case MsgPackCode.Int8:
                case MsgPackCode.UInt8:
                    _pos++;
                    break;

                case MsgPackCode.Int16:
                case MsgPackCode.UInt16:
                    _pos += 2;
                    break;

                case MsgPackCode.Int32:
                case MsgPackCode.UInt32:
                case MsgPackCode.Float32:
                    _pos += 4;
                    break;

                case MsgPackCode.Int64:
                case MsgPackCode.UInt64:
                case MsgPackCode.Float64:
                    _pos += 8;
                    break;

                case MsgPackCode.Bin8:
                case MsgPackCode.Str8:
                    _pos += _span[_pos] + 1;
                    break;

                case MsgPackCode.Bin16:
                case MsgPackCode.Str16:
                    _pos = BinaryPrimitives.ReadUInt16BigEndian(GetSpan(2)) + _pos;
                    break;

                case MsgPackCode.Bin32:
                case MsgPackCode.Str32:
                    _pos = checked((int)BinaryPrimitives.ReadUInt32BigEndian(GetSpan(4))) + _pos;
                    break;

                case MsgPackCode.FixExt1:
                    _pos += 2;
                    break;

                case MsgPackCode.FixExt2:
                    _pos += 3;
                    break;

                case MsgPackCode.FixExt4:
                    _pos += 5;
                    break;

                case MsgPackCode.FixExt8:
                    _pos += 9;
                    break;

                case MsgPackCode.FixExt16:
                    _pos += 17;
                    break;

                case MsgPackCode.Ext8:
                    _pos += _span[_pos] + 2;
                    break;

                case MsgPackCode.Ext16:
                    _pos = BinaryPrimitives.ReadUInt16BigEndian(GetSpan(2)) + 1 + _pos;
                    break;

                case MsgPackCode.Ext32:
                    _pos = checked((int)BinaryPrimitives.ReadUInt32BigEndian(GetSpan(4))) + 1 + _pos;
                    break;

                case MsgPackCode.Array16:
                    count += BinaryPrimitives.ReadUInt16BigEndian(GetSpan(2));
                    break;

                case MsgPackCode.Array32:
                    count += checked((int)BinaryPrimitives.ReadUInt32BigEndian(GetSpan(4)));
                    break;

                case MsgPackCode.Map16:
                    count += BinaryPrimitives.ReadUInt16BigEndian(GetSpan(2)) * 2;
                    break;

                case MsgPackCode.Map32:
                    count += checked((int)BinaryPrimitives.ReadUInt32BigEndian(GetSpan(4)) * 2);
                    break;

                default:
                    throw GetInvalidCodeException("valid type code", code);
            }
        }
    }

    /// <summary>
    /// Reads <see cref="ColumnType"/> and value with optional marshaller.
    /// </summary>
    /// <param name="marshaller">Optional marshaller.</param>
    /// <returns>Value.</returns>
    /// <typeparam name="T">Type of the value.</typeparam>
    public T ReadObjectFromBinaryTuple<T>(IMarshaller<T>? marshaller)
    {
        var obj = ReadObjectFromBinaryTuple();

        if (marshaller == null || obj == null)
        {
            return (T)obj!;
        }

        // TODO: Avoid allocating byte array, pass a span from BinaryTupleReader.
        if (obj is byte[] bytes)
        {
            return marshaller.Unmarshal(bytes);
        }

        throw new UnsupportedObjectTypeMarshallingException(
            Guid.NewGuid(),
            ErrorGroups.Marshalling.UnsupportedObjectType,
            "Unsupported object type. Expected byte[], got " + obj.GetType());
    }

    /// <summary>
    /// Reads <see cref="ColumnType"/> and value.
    /// </summary>
    /// <returns>Value.</returns>
    public object? ReadObjectFromBinaryTuple()
    {
        if (TryReadNil())
        {
            return null;
        }

        var tuple = new BinaryTupleReader(ReadBinary(), 3);

        return tuple.GetObject(0);
    }

    private static InvalidDataException GetInvalidCodeException(string expected, byte code) =>
        new($"Invalid code, expected '{expected}', but got '{code}'");

    private ReadOnlySpan<byte> GetSpan(int length)
    {
        var span = _span.Slice(_pos, length);

        _pos += length;

        return span;
    }

    private void CheckCode(string name, byte expectedCode)
    {
        var code = _span[_pos++];
        if (code != expectedCode)
        {
            throw GetInvalidCodeException(name, code);
        }
    }
}
