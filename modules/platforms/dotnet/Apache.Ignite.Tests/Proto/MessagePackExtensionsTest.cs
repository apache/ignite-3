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
    using System;
    using Internal.Buffers;
    using Internal.Proto;
    using MessagePack;
    using NUnit.Framework;

    /// <summary>
    /// Tests for <see cref="MessagePackWriterExtensions"/> and <see cref="MessagePackReaderExtensions"/>.
    /// </summary>
    public class MessagePackExtensionsTest
    {
        private static readonly string?[] TestStrings =
        {
            "foo",
            string.Empty,
            null,
            "тест",
            "ascii0123456789",
            "的的abcdкириллица",
            new(new[] {(char) 0xD801, (char) 0xDC37}),
        };

        private static readonly Guid[] TestGuids =
        {
            Guid.Empty, new(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11), Guid.NewGuid()
        };

        [Test]
        public void TestString()
        {
            foreach (var val in TestStrings)
            {
                var res = WriteRead(
                    buf =>
                    {
                        var w = buf.GetMessageWriter();

                        w.Write(val);
                        w.Flush();
                    },
                    m => new MessagePackReader(m).ReadString());

                Assert.AreEqual(val, res);
            }
        }

        [Test]
        public void TestGuid()
        {
            foreach (var guid in TestGuids)
            {
                var res = WriteRead(
                    buf =>
                    {
                        var w = buf.GetMessageWriter();

                        w.Write(guid);
                        w.Flush();
                    },
                    m =>
                    {
                        var r = new MessagePackReader(m);

                        return r.ReadGuid();
                    });

                Assert.AreEqual(guid, res);
            }
        }

        private static T WriteRead<T>(Action<PooledArrayBufferWriter> write, Func<ReadOnlyMemory<byte>, T> read)
        {
            var bufferWriter = new PooledArrayBufferWriter();
            write(bufferWriter);

            var mem = bufferWriter.GetWrittenMemory()[4..]; // Skip length.
            return read(mem);
        }
    }
}
