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

namespace Apache.Ignite.Tests.Table.Serialization;

using System;
using Internal.Table.Serialization;
using NUnit.Framework;

/// <summary>
/// Tests that different serializers produce consistent results.
/// </summary>
public class SerializerHandlerConsistencyTests
{
    // TODO:
    // * Use schema with interleaved columns
    // * Check full and key-only modes
    // * Check hashing
    [Test]
    public void TestConsistency()
    {
        var tupleHandler = TupleSerializerHandler.Instance;
        var tupleKvHandler = TuplePairSerializerHandler.Instance;
        var objectHandler = new ObjectSerializerHandler<Poco>();
        var objectKvHandler = new ObjectSerializerHandler<KvPair<PocoKey, PocoVal>>();
    }

    private class Poco
    {
        public string? Val1 { get; set; }

        public int Key1 { get; set; }

        public Guid Val2 { get; set; }

        public string Key2 { get; set; } = string.Empty;
    }

    private class PocoKey
    {
        public int Key1 { get; set; }

        public string Key2 { get; set; } = string.Empty;
    }

    private class PocoVal
    {
        public string? Val1 { get; set; }

        public Guid Val2 { get; set; }
    }
}
