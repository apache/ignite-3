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
using System.Reflection;
using Internal.Buffers;
using Internal.Proto;
using Internal.Proto.MsgPack;
using MessagePack;
using NUnit.Framework;

using static MsgPackTestsCommon;

/// <summary>
/// Tests for <see cref="MsgPackWriter"/>.
/// </summary>
public class MsgPackWriterTests
{
    private static readonly GetBytesLength GetBytesLengthAccessor = typeof(MessagePackReader)
        .GetMethod(nameof(GetBytesLength), BindingFlags.Instance | BindingFlags.NonPublic)!
        .CreateDelegate<GetBytesLength>();

    private delegate int GetBytesLength(ref MessagePackReader reader);

    [Test]
    public void TestWriteNil()
    {
        var res = Write(x => x.MessageWriter.WriteNil());

        Assert.AreEqual(1, res.Length);
        Assert.AreEqual(MsgPackCode.Nil, res[0]);
    }

    [Test]
    public void TestWriteBool()
    {
        var res = Write(x =>
        {
            x.MessageWriter.Write(true);
            x.MessageWriter.Write(false);
        });

        Assert.AreEqual(2, res.Length);
        Assert.AreEqual(MsgPackCode.True, res[0]);
        Assert.AreEqual(MsgPackCode.False, res[1]);
    }

    [Test]
    public void TestWriteUnsigned()
    {
        foreach (var number in GetNumbers())
        {
            var buf = new byte[20];
            MsgPackWriter.WriteUnsigned(buf.AsSpan(), (ulong)number);

            Assert.AreEqual((ulong)number, new MessagePackReader(buf.AsMemory()).ReadUInt64());
        }
    }

    [Test]
    public void TestWriteLong()
    {
        foreach (var number in GetNumbers())
        {
            var res = Write(x => x.MessageWriter.Write(number));

            Assert.AreEqual(number, new MessagePackReader(res.AsMemory()).ReadInt64());
        }
    }

    [Test]
    public void TestWriteInt()
    {
        foreach (var number in GetNumbers(int.MaxValue))
        {
            var res = Write(x => x.MessageWriter.Write((int)number));

            Assert.AreEqual(number, new MessagePackReader(res.AsMemory()).ReadInt32());
        }
    }

    [Test]
    public void TestWriteBitSet()
    {
        var res = Write(x =>
        {
            var span = x.MessageWriter.WriteBitSet(12);
            span[1] = 1;
        });

        Assert.AreEqual(4, res.Length);
        Assert.AreEqual(MsgPackCode.FixExt2, res[0]);
        Assert.AreEqual((byte)ClientMessagePackType.Bitmask, res[1]);
        Assert.AreEqual(0, res[2]);
        Assert.AreEqual(1, res[3]);
    }

    [Test]
    public void TestWriteExtensionHeader()
    {
        foreach (var number in GetNumbers(int.MaxValue / 2, unsignedOnly: true))
        {
            var res = Write(x => x.MessageWriter.WriteExtensionHeader(1, (int)number));
            var readRes = new MessagePackReader(res.AsMemory()).TryReadExtensionFormatHeader(out var hdr);

            Assert.IsTrue(readRes);
            Assert.AreEqual(1, hdr.TypeCode);
            Assert.AreEqual(number, hdr.Length);
        }
    }

    [Test]
    public void TestWriteBinaryHeader()
    {
        foreach (var number in GetNumbers(int.MaxValue / 2, unsignedOnly: true))
        {
            var res = Write(x => x.MessageWriter.WriteBinaryHeader((int)number));
            var reader = new MessagePackReader(res.AsMemory());
            var len = GetBytesLengthAccessor(ref reader);

            Assert.AreEqual(number, len);
        }
    }

    [Test]
    public void TestWriteBin()
    {
        var bytes = new byte[] { 1, 2, 3 };
        var res = Write(x => x.MessageWriter.Write(bytes));
        var resBytes = new MessagePackReader(res.AsMemory()).ReadBytes();

        CollectionAssert.AreEqual(bytes, resBytes!.Value.ToArray());
    }

    [Test]
    public void TestWriteString()
    {
        foreach (var str in TestStrings)
        {
            var res = Write(x => x.MessageWriter.Write(str));

            Assert.AreEqual(str, new MessagePackReader(res.AsMemory()).ReadString());
        }
    }

    private static byte[] Write(Action<PooledArrayBuffer> writer)
    {
        using var buf = new PooledArrayBuffer();
        writer(buf);

        return buf.GetWrittenMemory().ToArray();
    }
}
