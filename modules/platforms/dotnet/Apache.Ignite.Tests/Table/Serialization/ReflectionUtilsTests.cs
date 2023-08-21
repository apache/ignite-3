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

// ReSharper disable InconsistentNaming, UnusedMember.Local
#pragma warning disable SA1306, SA1401, CS0649, CS0169, CA1823, SA1201
namespace Apache.Ignite.Tests.Table.Serialization
{
    using System;
    using System.Collections.Generic;
    using System.ComponentModel.DataAnnotations.Schema;
    using System.Linq;
    using System.Reflection;
    using Internal.Table.Serialization;
    using NUnit.Framework;

    /// <summary>
    /// Tests for <see cref="ReflectionUtils"/>.
    /// </summary>
    public class ReflectionUtilsTests
    {
        [Test]
        public void TestGetFieldByColumnNameReturnsFieldByName() =>
            Assert.AreEqual("BaseFieldPublic", GetFieldByColumnName(typeof(Derived), "BaseFieldPublic")!.Name);

        [Test]
        public void TestGetFieldByColumnNameReturnsFieldByPropertyName() =>
            Assert.AreEqual("<BaseProp>k__BackingField", GetFieldByColumnName(typeof(Derived), "BaseProp")!.Name);

        [Test]
        public void TestGetFieldByColumnNameReturnsFieldByColumnAttributeName() =>
            Assert.AreEqual("BaseFieldCustomColumnName", GetFieldByColumnName(typeof(Derived), "FldCol")!.Name);

        [Test]
        public void TestGetFieldByColumnNameReturnsFieldByPropertyColumnAttributeName() =>
            Assert.AreEqual("<BasePropCustomColumnName>k__BackingField", GetFieldByColumnName(typeof(Derived), "PropCol")!.Name);

        [Test]
        public void TestGetFieldByColumnNameReturnsNullForNonMatchingName() =>
            Assert.IsNull(GetFieldByColumnName(typeof(Derived), "foo"));

        [Test]
        public void TestGetFieldByColumnNameReturnsNullForNotMappedProperty() =>
            Assert.IsNull(GetFieldByColumnName(typeof(Derived), "NotMappedProp"));

        [Test]
        public void TestGetFieldByColumnNameReturnsNullForNotMappedField() =>
            Assert.IsNull(GetFieldByColumnName(typeof(Derived), "NotMappedFld"));

        [Test]
        public void TestGetFieldByColumnNameThrowsExceptionForDuplicateColumnName()
        {
            var ex1 = Assert.Throws<ArgumentException>(() => GetFieldByColumnName(typeof(DuplicateColumn1), "foo"));
            var ex2 = Assert.Throws<ArgumentException>(() => GetFieldByColumnName(typeof(DuplicateColumn2), "foo"));

            var expected1 = "Column 'MyCol' maps to more than one field of type " +
                            "Apache.Ignite.Tests.Table.Serialization.ReflectionUtilsTests+DuplicateColumn1: " +
                            "Int32 <MyCol2>k__BackingField and Int32 <MyCol>k__BackingField";

            var expected2 = "Column 'MyCol' maps to more than one field of type " +
                            "Apache.Ignite.Tests.Table.Serialization.ReflectionUtilsTests+DuplicateColumn2: " +
                            "Int32 <MyCol2>k__BackingField and Int32 <MyCol1>k__BackingField";

            Assert.AreEqual(expected1, ex1!.Message);
            Assert.AreEqual(expected2, ex2!.Message);
        }

        [Test]
        public void TestGetColumns()
        {
            var columns = typeof(Columns).GetColumns().ToArray();

            Assert.AreEqual(4, columns.Length);

            Assert.AreEqual("Col1", columns[0].Name);
            Assert.IsFalse(columns[0].HasColumnNameAttribute);

            Assert.AreEqual("Column 2", columns[1].Name);
            Assert.IsTrue(columns[1].HasColumnNameAttribute);

            Assert.AreEqual("Col3", columns[2].Name);
            Assert.IsFalse(columns[2].HasColumnNameAttribute);

            Assert.AreEqual("Column 4", columns[3].Name);
            Assert.IsTrue(columns[3].HasColumnNameAttribute);
        }

        [Test]
        public void TestGetAllFieldsIncludesPrivatePublicAndInherited()
        {
            var fields = ReflectionUtilsGetAllFields(typeof(Derived)).Select(f => f.Name).OrderBy(x => x).ToArray();

            var expected = new[]
            {
                "<BaseProp>k__BackingField",
                "<BasePropCustomColumnName>k__BackingField",
                "<BaseTwoProp>k__BackingField",
                "<DerivedProp>k__BackingField",
                "<NotMappedProp>k__BackingField",
                "BaseFieldCustomColumnName",
                "BaseFieldInternal",
                "BaseFieldPrivate",
                "BaseFieldProtected",
                "BaseFieldPublic",
                "BaseTwoFieldInternal",
                "BaseTwoFieldPrivate",
                "BaseTwoFieldProtected",
                "BaseTwoFieldPublic",
                "DerivedFieldInternal",
                "DerivedFieldPrivate",
                "DerivedFieldProtected",
                "DerivedFieldPublic",
                "NotMappedFld"
            };

            CollectionAssert.AreEqual(expected, fields);
        }

        [Test]
        public void TestCleanFieldNameReturnsPropertyNameForBackingField()
        {
            var fieldNames = ReflectionUtilsGetAllFields(typeof(Derived)).Select(f => ReflectionUtilsCleanFieldName(f.Name)).ToArray();

            CollectionAssert.Contains(fieldNames, nameof(Base.BaseProp));
            CollectionAssert.Contains(fieldNames, nameof(BaseTwo.BaseTwoProp));
            CollectionAssert.Contains(fieldNames, nameof(Derived.DerivedProp));
        }

        [Test]
        [TestCase("Field", "Field")]
        [TestCase("_foo", "_foo")]
        [TestCase("m_fooBar", "m_fooBar")]
        [TestCase("<MyProperty>k__BackingField", "MyProperty")]
        [TestCase("FSharpProp@", "FSharpProp")]
        [TestCase("<AnonTypeProp>i__Field", "AnonTypeProp")]
        public void TestCleanFieldName(string name, string expected)
        {
            Assert.AreEqual(expected, ReflectionUtilsCleanFieldName(name));
        }

        private static FieldInfo? GetFieldByColumnName(Type type, string name) =>
            ReflectionUtils.GetFieldsByColumnName(type).TryGetValue(name, out var fieldInfo) ? fieldInfo.Field : null;

        private static string? ReflectionUtilsCleanFieldName(string name) =>
            typeof(ReflectionUtils).GetMethod("CleanFieldName", BindingFlags.Static | BindingFlags.NonPublic)!
                .Invoke(null, new object[] { name }) as string;

        private static IEnumerable<FieldInfo> ReflectionUtilsGetAllFields(Type type) =>
            (IEnumerable<FieldInfo>)typeof(ReflectionUtils).GetMethod("GetAllFields", BindingFlags.Static | BindingFlags.NonPublic)!
                .Invoke(null, new object[] { type })!;

        private class Base
        {
            public int BaseFieldPublic;
            internal int BaseFieldInternal;
            protected int BaseFieldProtected;
            private int BaseFieldPrivate;

            public int BaseProp { get; set; }

            [Column("PropCol")]
            public int BasePropCustomColumnName { get; set; }

            [Column("FldCol")]
            public int BaseFieldCustomColumnName;

            [NotMapped]
            public int NotMappedFld;

            [NotMapped]
            public int NotMappedProp { get; set; }
        }

        private class BaseTwo : Base
        {
            public int BaseTwoFieldPublic;
            internal int BaseTwoFieldInternal;
            protected int BaseTwoFieldProtected;
            private int BaseTwoFieldPrivate;

            public int BaseTwoProp { get; set; }
        }

        private class Derived : BaseTwo
        {
            public int DerivedFieldPublic;
            internal int DerivedFieldInternal;
            protected int DerivedFieldProtected;
            private int DerivedFieldPrivate;

            public int DerivedProp { get; set; }
        }

        private class DuplicateColumn1
        {
            public int MyCol { get; set; }

            [Column("MyCol")]
            public int MyCol2 { get; set; }
        }

        private class DuplicateColumn2
        {
            [Column("MyCol")]
            public int MyCol1 { get; set; }

            [Column("MyCol")]
            public int MyCol2 { get; set; }
        }

        private class Columns
        {
            public int Col1;

            [Column("Column 2")]
            public int Col2;

            public int Col3 { get; set; }

            [Column("Column 4")]
            public int Col4 { get; set; }

            [NotMapped]
            public int Col5 { get; set; }
        }
    }
}
