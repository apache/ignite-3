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

// ReSharper disable InconsistentNaming
// ReSharper disable UnusedMember.Local
#pragma warning disable SA1306, SA1401, CS0649, CS0169, CA1823, CA1812, SA1201
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
        public void TestGetFieldByColumnNameReturnsFieldByName()
        {
            var res = typeof(Derived).GetFieldByColumnName("BaseFieldPublic");
            Assert.AreEqual("BaseFieldPublic", res!.Name);
        }

        [Test]
        public void TestGetFieldByColumnNameReturnsFieldByPropertyName()
        {
            var res = typeof(Derived).GetFieldByColumnName("BaseProp");
            Assert.AreEqual("<BaseProp>k__BackingField", res!.Name);
        }

        [Test]
        public void TestGetFieldByColumnNameReturnsFieldByColumnAttributeName()
        {
            var res = typeof(Derived).GetFieldByColumnName("FldCol");
            Assert.AreEqual("BaseFieldCustomColumnName", res!.Name);
        }

        [Test]
        public void TestGetFieldByColumnNameReturnsFieldByPropertyColumnAttributeName()
        {
            var res = typeof(Derived).GetFieldByColumnName("PropCol");
            Assert.AreEqual("<BasePropCustomColumnName>k__BackingField", res!.Name);
        }

        [Test]
        public void TestGetFieldByColumnNameReturnsNullForNonMatchingName()
        {
            Assert.IsNull(typeof(Derived).GetFieldByColumnName("foo"));
        }

        [Test]
        public void TestGetFieldByColumnNameReturnsNullForNotMappedProperty() =>
            Assert.IsNull(typeof(Derived).GetFieldByColumnName("NotMappedProp"));

        [Test]
        public void TestGetFieldByColumnNameReturnsNullForNotMappedField() =>
            Assert.IsNull(typeof(Derived).GetFieldByColumnName("NotMappedFld"));

        [Test]
        public void TestGetFieldByColumnNameThrowsExceptionForDuplicateColumnName()
        {
        }

        [Test]
        public void TestGetColumns()
        {
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
    }
}
