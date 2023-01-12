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
using Internal.Buffers;
using Internal.Proto.MsgPack;
using MessagePack;
using NUnit.Framework;

using static MsgPackTestsCommon;

/// <summary>
/// Tests for <see cref="MsgPackWriter"/>.
/// </summary>
public class MsgPackWriterTests
{
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

            var reader = new MessagePackReader(buf.AsMemory());
            Assert.AreEqual((ulong)number, reader.ReadUInt64());
        }
    }

    private static byte[] Write(Action<PooledArrayBuffer> writer)
    {
        using var buf = new PooledArrayBuffer();
        buf.MessageWriter.WriteNil();

        return buf.GetWrittenMemory().ToArray();
    }
}
