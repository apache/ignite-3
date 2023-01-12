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

namespace Apache.Ignite.Tests.Buffers
{
    using Internal.Buffers;
    using NUnit.Framework;

    /// <summary>
    /// Tests for <see cref="PooledArrayBuffer"/>.
    /// </summary>
    public class PooledArrayBufferWriterTests
    {
        [Test]
        public void TestBufferWriterReservesPrefixSpace()
        {
            using var bufferWriter = new PooledArrayBuffer();
            var writer = bufferWriter.MessageWriter;

            writer.Write(1);
            writer.Write("A");

            var res = bufferWriter.GetWrittenMemory().ToArray();

            CollectionAssert.AreEqual(new byte[] { 1, 0xa1, (byte)'A' }, res);
        }
    }
}
