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
// ReSharper disable UnusedMember.Local
// ReSharper disable UnusedAutoPropertyAccessor.Local
// ReSharper disable AutoPropertyCanBeMadeGetOnly.Local
namespace Apache.Ignite.Tests.Table;

using System;
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

    [SetUp]
    public async Task InsertData()
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
    public async Task TestRecordPropertyMapping()
    {
        var res = await Table.GetRecordView<PropertyMapping>().GetAsync(null, new PropertyMapping(Key));
        Assert.AreEqual(Val, res.Value.Name);
    }

    [Test]
    public async Task TestClassPropertyMapping()
    {
        var res = await Table.GetRecordView<ClassPropertyMapping>().GetAsync(null, new ClassPropertyMapping { Id = Key });
        Assert.AreEqual(Val, res.Value.Name);
    }

    [Test]
    public async Task TestStructFieldMapping()
    {
        var res = await Table.GetRecordView<StructFieldMapping>().GetAsync(null, new StructFieldMapping(Key));
        Assert.AreEqual(Val, res.Value.Name);
    }

    [Test]
    public async Task TestStructPropertyMapping()
    {
        var res = await Table.GetRecordView<StructPropertyMapping>().GetAsync(null, new StructPropertyMapping(Key));
        Assert.AreEqual(Val, res.Value.Name);
    }

    [Test]
    public void TestComputedPropertyMappingThrowsException()
    {
        var ex = Assert.ThrowsAsync<ArgumentException>(async () =>
            await Table.GetRecordView<ComputedPropertyMapping>().GetAsync(null, new ComputedPropertyMapping { Id = Key }));

        Assert.AreEqual(
            "Can't map 'Apache.Ignite.Tests.Table.RecordViewCustomMappingTest+ComputedPropertyMapping' to columns" +
            " 'Int64 KEY, String VAL'. Matching fields not found.",
            ex!.Message);
    }

    [Test]
    public void TestDuplicateColumnNameMappingThrowsException()
    {
        var ex = Assert.ThrowsAsync<ArgumentException>(async () =>
            await Table.GetRecordView<FieldMappingDuplicate>().GetAsync(null, new FieldMappingDuplicate(Key)));

        Assert.AreEqual(
            "Column 'Val' maps to more than one field of type " +
            "Apache.Ignite.Tests.Table.RecordViewCustomMappingTest+FieldMappingDuplicate: " +
            "System.String <Name2>k__BackingField and " +
            "System.String <Name>k__BackingField",
            ex!.Message);
    }

    private record struct StructFieldMapping([field: Column("Key")] long Id, [field: Column("Val")] string? Name = null);

    private record struct StructPropertyMapping([property: Column("KEY")] long Id, [property: Column("VAL")] string? Name = null);

    private record FieldMapping([field: Column("Key")] long Id, [field: Column("Val")] string? Name = null);

    private record PropertyMapping([property: Column("Key")] long Id, [property: Column("Val")] string? Name = null);

    private class ClassPropertyMapping
    {
        [Column("key")]
        public long Id { get; set; }

        [Column("VAL")]
        public string Name { get; set; } = null!;
    }

    // ReSharper disable MemberHidesStaticFromOuterClass
    private record ComputedPropertyMapping
    {
        public long Id { get; set; }

        public string? Name { get; set; }

        public long Key
        {
            get => Id;
            set => Id = value;
        }

        public string? Val
        {
            get => Name;
            set => Name = value;
        }
    }

    private record FieldMappingDuplicate(long Key, [field: Column("Val")] string? Name = null, [field: Column("Val")] string? Name2 = null);
}
