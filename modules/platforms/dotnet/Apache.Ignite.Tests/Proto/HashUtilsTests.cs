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

namespace Apache.Ignite.Tests.Proto;

using System;
using System.Buffers.Binary;
using Internal.Buffers;
using Internal.Proto;
using Internal.Proto.MsgPack;
using MessagePack;
using NUnit.Framework;

public class HashUtilsTests
{
    [Test]
    public void Test1() // TODO: remove
    {
        int key1 = 1;
        int key2 = 2;

        var hash1 = HashUtils.Hash32(key1, 0);
        var hash2 = HashUtils.Hash32(key2, 0);

        Span<byte> dataToHash = new byte[8];
        BinaryPrimitives.WriteUInt32LittleEndian(dataToHash, (uint)(key1 & 0xffffffffL));
        BinaryPrimitives.WriteUInt32LittleEndian(dataToHash[4..], (uint)(key2 & 0xffffffffL));

        var combinedHash1 = HashUtils.Hash32(2, hash1);
        var hash2_1 = HashUtils.Hash32(dataToHash[..4], 0);

        // var combinedHash2 = HashUtils.Hash32(dataToHash[8..], hash2_1);
        var combinedHash2 = HashUtils.Hash32(dataToHash, 0);
        var combinedHash3 = HashUtils.Hash32(dataToHash[4..], hash2_1);

        Assert.AreEqual(combinedHash1, combinedHash3);
    }

    [Test]
    public void Test2()
    {
        var val = (short)1;
        var hash1 = HashUtils.Hash32(val, 0);

        using var buf = new PooledArrayBuffer();
        HashUtils.WriteHashBytes(val, buf.MessageWriter);
        var hashes = buf.GetWrittenMemory();
        var hashReader = new MsgPackReader(hashes.Span);
        var hashBytes = hashReader.ReadBinary();
        var hash2 = HashUtils.Hash32(hashBytes, 0);

        Assert.AreEqual(hash1, hash2);
    }
}
