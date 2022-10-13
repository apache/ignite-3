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

namespace Apache.Ignite.Tests.Table;

using System.Linq;
using System.Threading.Tasks;
using NUnit.Framework;

/// <summary>
/// Tests for key-value tuple view.
/// </summary>
public class KeyValueViewBinaryTests : IgniteTestsBase
{
    [TearDown]
    public async Task CleanTable()
    {
        await TupleView.DeleteAllAsync(null, Enumerable.Range(-1, 12).Select(x => GetTuple(x)));
    }

    [Test]
    public async Task TestPutGet()
    {
        var kvView = Table.KeyValueBinaryView;

        await kvView.PutAsync(null, GetTuple(1L), GetTuple("val"));

        var (res, _) = await kvView.GetAsync(null, GetTuple(1L));

        Assert.AreEqual("val", res[0]);
        Assert.AreEqual("val", res[ValCol]);
    }
}