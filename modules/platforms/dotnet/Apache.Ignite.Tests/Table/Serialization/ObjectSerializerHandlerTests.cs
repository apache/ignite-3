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

namespace Apache.Ignite.Tests.Table.Serialization
{
    using System;
    using Internal.Buffers;
    using Internal.Proto;
    using Internal.Proto.BinaryTuple;
    using Internal.Table;
    using Internal.Table.Serialization;
    using MessagePack;
    using NUnit.Framework;

    /// <summary>
    /// Tests for <see cref="ObjectSerializerHandler{T}"/>.
    /// </summary>
    // ReSharper disable NotAccessedPositionalProperty.Local
    public class ObjectSerializerHandlerTests
    {
        private static readonly Schema Schema = new(1, 1, new[]
        {
            new Column("Key", ClientDataType.Int64, false, true, 0),
            new Column("Val", ClientDataType.String, false, false, 1)
        });

        [Test]
        public void TestWrite()
        {
            var reader = WriteAndGetTupleReader();

            Assert.AreEqual(1234, reader.GetInt(0));
            Assert.AreEqual("foo", reader.GetString(1));
        }

        [Test]
        public void TestWriteKeyOnly()
        {
            var reader = WriteAndGetTupleReader(keyOnly: true);

            Assert.AreEqual(1234, reader.GetInt(0));
        }

        [Test]
        public void TestRead()
        {
            var reader = WriteAndGetReader();
            var resPoco = new ObjectSerializerHandler<Poco>().Read(ref reader, Schema);

            Assert.AreEqual(1234, resPoco.Key);
            Assert.AreEqual("foo", resPoco.Val);
        }

        [Test]
        public void TestReadKeyOnly()
        {
            var reader = WriteAndGetReader(true);
            var resPoco = new ObjectSerializerHandler<Poco>().Read(ref reader, Schema, keyOnly: true);

            Assert.AreEqual(1234, resPoco.Key);
            Assert.IsNull(resPoco.Val);
        }

        [Test]
        public void TestReadUnsupportedFieldTypeThrowsException()
        {
            var ex = Assert.Throws<IgniteClientException>(() =>
            {
                var reader = WriteAndGetReader();
                new ObjectSerializerHandler<BadPoco>().Read(ref reader, Schema);
            });

            Assert.AreEqual(
                "Can't map field 'BadPoco.<Key>k__BackingField' of type 'System.Guid'" +
                " to column 'Key' of type 'System.Int64' - types do not match.",
                ex!.Message);
        }

        [Test]
        public void TestWriteUnsupportedFieldTypeThrowsException()
        {
            var ex = Assert.Throws<IgniteClientException>(() => Write(new BadPoco(Guid.Empty, DateTimeOffset.Now)));

            Assert.AreEqual(
                "Can't map field 'BadPoco.<Key>k__BackingField' of type 'System.Guid'" +
                " to column 'Key' of type 'System.Int64' - types do not match.",
                ex!.Message);
        }

        private static MessagePackReader WriteAndGetReader(bool keyOnly = false)
        {
            var bytes = Write(new Poco { Key = 1234, Val = "foo" }, keyOnly);

            return new MessagePackReader(bytes);
        }

        private static BinaryTupleReader WriteAndGetTupleReader(bool keyOnly = false)
        {
            var msgPackReader = WriteAndGetReader(keyOnly);
            var bytes = msgPackReader.ReadBytesAsMemory();

            return new BinaryTupleReader(bytes, keyOnly ? 1 : 2);
        }

        private static byte[] Write<T>(T obj, bool keyOnly = false)
            where T : class
        {
            var handler = new ObjectSerializerHandler<T>();

            using var pooledWriter = new PooledArrayBufferWriter();
            var writer = pooledWriter.GetMessageWriter();

            handler.Write(ref writer, Schema, obj, keyOnly);
            writer.Flush();

            // Slice NoValueSet.
            return pooledWriter.GetWrittenMemory().Slice(3).ToArray();
        }

        private record BadPoco(Guid Key, DateTimeOffset Val);
    }
}
