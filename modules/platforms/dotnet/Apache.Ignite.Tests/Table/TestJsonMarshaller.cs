// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

namespace Apache.Ignite.Tests.Table;

using System;
using System.Buffers;
using System.Text;
using Ignite.Marshalling;

/// <summary>
/// Test JSON marshaller.
/// </summary>
/// <typeparam name="T">Element type.</typeparam>
public class TestJsonMarshaller<T> : IMarshaller<T>
{
    private readonly JsonMarshaller<T> _marshaller;

    public TestJsonMarshaller(JsonMarshaller<T> marshaller) => _marshaller = marshaller;

    public void Marshal(T obj, IBufferWriter<byte> writer) => _marshaller.Marshal(obj, writer);

    public T Unmarshal(ReadOnlySpan<byte> bytes)
    {
        T res;

        try
        {
            res = _marshaller.Unmarshal(bytes);
        }
        catch (Exception e)
        {
            throw new InvalidOperationException($"Failed to deserialize JSON: '{Encoding.UTF8.GetString(bytes)}'", e);
        }

        if (res != null && res.ToString()!.Contains("error", StringComparison.OrdinalIgnoreCase))
        {
            throw new InvalidOperationException("Test marshaller error: " + res);
        }

        return res;
    }
}
