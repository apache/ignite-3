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

using System.Collections.Generic;
using System.Threading.Tasks;
using Internal.Proto.BinaryTuple;
using NUnit.Framework;

/// <summary>
/// Tests that colocation hash calculation is consistent with server logic.
/// </summary>
public class ColocationHashTests : IgniteTestsBase
{
    private const string ColocationHashJob = "org.apache.ignite.internal.runner.app.PlatformTestNodeRunner$ColocationHashJob";

    [Test]
    public async Task TestColocationHashIsSameOnServerAndClient()
    {
        var keys = new object[] { 1, 2L };
        var (bytes, hash) = WriteAsBinaryTuple(keys);

        var serverHash = await GetServerHash(bytes, 2);

        Assert.AreEqual(hash, serverHash);
    }

    private static (byte[] Bytes, int Hash) WriteAsBinaryTuple(IReadOnlyCollection<object> arr)
    {
        using var builder = new BinaryTupleBuilder(arr.Count * 3, hashedColumnsPredicate: new TestIndexProvider());

        foreach (var obj in arr)
        {
            builder.AppendObjectWithType(obj);
        }

        return (builder.Build().ToArray(), builder.Hash);
    }

    private async Task<int> GetServerHash(byte[] bytes, int count)
    {
        var nodes = await Client.GetClusterNodesAsync();

        return await Client.Compute.ExecuteAsync<int>(nodes, ColocationHashJob, count, bytes);
    }

    private class TestIndexProvider : IHashedColumnIndexProvider
    {
        public bool IsHashedColumnIndex(int index) => index % 3 == 2;
    }
}
