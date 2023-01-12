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

namespace Apache.Ignite.Tests.Proto.MsgPack;

using System;
using System.Buffers;
using System.Linq;
using Internal.Buffers;
using Internal.Proto.MsgPack;
using MessagePack;
using NUnit.Framework;

using static MsgPackTestsCommon;

/// <summary>
/// Tests for <see cref="MsgPackReader"/>.
/// </summary>
public class MsgPackReaderTests
{
    /** Random UUID string from Java. */
    private const string JavaUuidString = "6f24146a-244a-4018-a36c-3e9cf5b42082";

    /** Byte representation of the UUID above, serialized by Java ClientMessagePacker. */
    private static readonly sbyte[] JavaUuidBytes =
    {
        -40, 3, 24, 64, 74, 36, 106, 20, 36, 111, -126, 32, -76, -11, -100, 62, 108, -93
    };

    private static readonly Guid[] TestGuids =
    {
        Guid.Empty, new(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11), Guid.NewGuid()
    };

    [Test]
    public void TestReadArrayHeader()
    {
        var bufWriter = new ArrayBufferWriter<byte>();
        var writer = new MessagePackWriter(bufWriter);

        for (int i = 0; i < 30; i++)
        {
            writer.WriteArrayHeader((int)Math.Pow(2, i));
        }

        writer.Flush();

        var reader = new MsgPackReader(bufWriter.WrittenSpan);

        for (int i = 0; i < 30; i++)
        {
            Assert.AreEqual((int)Math.Pow(2, i), reader.ReadArrayHeader());
        }
    }

    [Test]
    public void TestTryReadNil()
    {
        var buf = new[] { MsgPackCode.Nil, MsgPackCode.Int8 };
        var reader = new MsgPackReader(buf);

        Assert.IsTrue(reader.TryReadNil());
        Assert.IsFalse(reader.TryReadNil());
        Assert.IsFalse(reader.TryReadNil());
    }

    [Test]
    public void TestReadPastBufferEndThrows()
    {
        var buf = new byte[] { MsgPackCode.Array16, 0, 0, 0, 0, 0, 0 };

        Assert.Throws<ArgumentOutOfRangeException>(() =>
        {
            // There is enough data in the array, but we take a smaller slice, which is not enough for Array16 header.
            var span = buf.AsSpan()[..2];
            var reader = new MsgPackReader(span);
            reader.ReadArrayHeader();
        });
    }

    [Test]
    public void TestReadStringNullable()
    {
        foreach (var val in TestStrings)
        {
            var res = WriteRead(
                buf => buf.MessageWriter.Write(val),
                m => new MsgPackReader(m.Span).ReadStringNullable());

            Assert.AreEqual(val, res);
        }
    }

    [Test]
    public void TestReadInt64()
    {
        foreach (var val in GetNumbers())
        {
            var res = WriteRead(
                buf => buf.MessageWriter.Write(val),
                m => new MsgPackReader(m.Span).ReadInt64());

            Assert.AreEqual(val, res);
        }
    }

    [Test]
    public void TestReadInt32()
    {
        foreach (var val in GetNumbers(int.MaxValue - 1))
        {
            var res = WriteRead(
                buf => buf.MessageWriter.Write(val),
                m => new MsgPackReader(m.Span).ReadInt32());

            Assert.AreEqual(val, res);
        }
    }

    [Test]
    public void TestReadInt16()
    {
        foreach (var val in GetNumbers(short.MaxValue - 1))
        {
            var res = WriteRead(
                buf => buf.MessageWriter.Write(val),
                m => new MsgPackReader(m.Span).ReadInt16());

            Assert.AreEqual(val, res);
        }
    }

    [Test]
    public void TestGuid()
    {
        foreach (var guid in TestGuids)
        {
            var res = WriteRead(
                buf => buf.MessageWriter.Write(guid),
                m =>
                {
                    var r = new MsgPackReader(m.Span);

                    return r.ReadGuid();
                });

            Assert.AreEqual(guid, res);
        }
    }

    [Test]
    public void TestReadJavaGuidReturnsIdenticalStringRepresentation()
    {
        var bytes = (byte[])(object)JavaUuidBytes;
        var mem = bytes.AsMemory();

        var reader = new MsgPackReader(mem.Span);
        var guid = reader.ReadGuid();

        Assert.AreEqual(JavaUuidString, guid.ToString());
    }

    [Test]
    public void TestWriteJavaGuidReturnsIdenticalByteRepresentation()
    {
        var bufferWriter = new PooledArrayBuffer();
        bufferWriter.MessageWriter.Write(Guid.Parse(JavaUuidString));

        var bytes = bufferWriter.GetWrittenMemory()
            .ToArray()
            .Select(b => (sbyte)b)
            .ToArray();

        CollectionAssert.AreEqual(JavaUuidBytes, bytes);
    }

    [Test]
    public void TestTryReadInt()
    {
        WriteRead(
            buf =>
            {
                var w = buf.MessageWriter;

                w.Write(3);
                w.Write(short.MaxValue);
                w.Write(int.MaxValue);
                w.Write("abc");
            },
            m =>
            {
                var r = new MsgPackReader(m.Span);
                int i;

                Assert.IsTrue(r.TryReadInt(out i));
                Assert.AreEqual(3, i);

                Assert.IsTrue(r.TryReadInt(out i));
                Assert.AreEqual(short.MaxValue, i);

                Assert.IsTrue(r.TryReadInt(out i));
                Assert.AreEqual(int.MaxValue, i);

                Assert.IsFalse(r.TryReadInt(out i));
                Assert.AreEqual("abc", r.ReadString());

                return new object();
            });
    }

    private static T WriteRead<T>(Action<PooledArrayBuffer> write, Func<ReadOnlyMemory<byte>, T> read)
    {
        var bufferWriter = new PooledArrayBuffer();
        write(bufferWriter);

        var mem = bufferWriter.GetWrittenMemory();
        return read(mem);
    }
}
