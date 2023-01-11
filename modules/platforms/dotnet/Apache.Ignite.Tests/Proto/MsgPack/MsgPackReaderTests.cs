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
using Internal.Proto.MsgPack;
using MessagePack;
using NUnit.Framework;

/// <summary>
/// Tests for <see cref="MsgPackReader"/>.
/// </summary>
public class MsgPackReaderTests
{
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
    }

    [Test]
    public void TestReadPastBufferEndThrows()
    {
        var arr = new byte[] { MsgPackCode.Array16, 0, 0, 0, 0, 0, 0 };

        Assert.Throws<ArgumentOutOfRangeException>(() =>
        {
            // There is enough data in the array, but we take a smaller slice, which is not enough for Array16 header.
            var span = arr.AsSpan()[..2];
            var reader = new MsgPackReader(span);
            reader.ReadArrayHeader();
        });
    }
}
