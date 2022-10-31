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

// ReSharper disable NotAccessedPositionalProperty.Local
namespace Apache.Ignite.Tests.Table;

using System.ComponentModel.DataAnnotations.Schema;
using System.Threading.Tasks;
using Ignite.Table;
using NUnit.Framework;

/// <summary>
/// Tests custom user type mapping behavior in <see cref="IRecordView{T}"/>.
/// </summary>
public class RecordViewCustomMappingTest : IgniteTestsBase
{
    private const long Key = 1;

    private const string Val = "val1";

    // TODO: classes, structs, records
    // TODO: Fields and properties
    // TODO: Properties without fields?
    [SetUp]
    public async Task SetUp()
    {
        await Table.RecordBinaryView.UpsertAsync(null, GetTuple(Key, Val));
    }

    [Test]
    public async Task TestFieldMapping()
    {
        var res = await Table.GetRecordView<FieldMapping>().GetAsync(null, new FieldMapping(Key));
        Assert.AreEqual(Val, res.Value.Name);
    }

    [Test]
    public async Task TestPropertyMapping()
    {
        // TODO
        await Task.Delay(1);
    }

    [Test]
    public async Task TestComputedPropertyMapping()
    {
        // TODO
        await Task.Delay(1);
    }

    private record FieldMapping([field: Column("Key")] long Id, [field: Column("Val")] string? Name = null);
}
