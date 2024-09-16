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

namespace Apache.Ignite.Tests.Marshalling;

using System.Buffers;
using System.Text;
using Ignite.Marshalling;
using NUnit.Framework;

/// <summary>
/// Tests for <see cref="JsonMarshaller{T}"/>.
/// </summary>
public class JsonMarshallerTests
{
    [Test]
    public void TestMarshalUnmarshal()
    {
        var marshaller = new JsonMarshaller<Poco>();
        var poco = new Poco(42, "foo");

        var writer = new ArrayBufferWriter<byte>();
        marshaller.Marshal(poco, writer);

        var bytes = writer.WrittenSpan.ToArray();
        var result = marshaller.Unmarshal(bytes);

        Assert.AreEqual(poco, result);
        Assert.AreEqual("{\"id\":42,\"name\":\"foo\"}", Encoding.UTF8.GetString(bytes));
    }

    [Test]
    public void TestToString()
    {
        Assert.AreEqual(
            "JsonMarshaller`1[Int32] { Options = System.Text.Json.JsonSerializerOptions }",
            new JsonMarshaller<int>().ToString());
    }

    private record Poco(int Id, string Name);
}
