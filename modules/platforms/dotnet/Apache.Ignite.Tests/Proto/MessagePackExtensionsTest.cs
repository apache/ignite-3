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

namespace Apache.Ignite.Tests.Proto
{
    using Internal.Buffers;
    using Internal.Proto;
    using MessagePack;
    using NUnit.Framework;

    /// <summary>
    /// Tests for <see cref="MessagePackWriterExtensions"/> and <see cref="MessagePackReaderExtensions"/>.
    /// </summary>
    public class MessagePackExtensionsTest
    {
        [Test]
        public void TestWriteString()
        {
            var bufferWriter = new PooledArrayBufferWriter();

            var w = bufferWriter.GetMessageWriter();

            w.WriteString("foo");
            w.Flush();

            var mem = bufferWriter.GetWrittenMemory().Slice(4); // Skip length.
            var reader = new MessagePackReader(mem);

            var res = reader.ReadString();

            Assert.AreEqual("foo", res);
        }
    }
}
