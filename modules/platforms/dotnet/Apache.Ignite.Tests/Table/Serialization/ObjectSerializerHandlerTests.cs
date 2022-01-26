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
    using System.Linq;
    using Internal.Buffers;
    using Internal.Proto;
    using Internal.Table;
    using Internal.Table.Serialization;
    using MessagePack;
    using NUnit.Framework;

    /// <summary>
    /// Tests for <see cref="ObjectSerializerHandler{T}"/>.
    /// </summary>
    public class ObjectSerializerHandlerTests
    {
        [Test]
        public void TestWritePocoType()
        {
            var poco = new Poco { Key = 1234, Val = "foo" };
            var handler = new ObjectSerializerHandler<Poco>();

            using var pooledWriter = new PooledArrayBufferWriter();
            var writer = pooledWriter.GetMessageWriter();

            var columns = new[]
            {
                new Column("Key", ClientDataType.Int64, false, true, 0),
                new Column("Val", ClientDataType.String, false, false, 1)
            };

            var columnsMap = columns.ToDictionary(x => x.Name);
            var schema = new Schema(1, 1, columns, columnsMap);

            handler.Write(ref writer, schema, poco);
            writer.Flush();

            var resMem = pooledWriter.GetWrittenMemory()[4..]; // Skip length header.
            var reader = new MessagePackReader(resMem);

            Assert.AreEqual(1234, reader.ReadInt32());
            Assert.AreEqual("foo", reader.ReadString());
        }
    }
}
