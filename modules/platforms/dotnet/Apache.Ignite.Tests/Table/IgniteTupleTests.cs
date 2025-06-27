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

namespace Apache.Ignite.Tests.Table
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using Ignite.Table;
    using NodaTime;
    using NUnit.Framework;

    /// <summary>
    /// Tests for <see cref="IIgniteTuple"/>.
    /// </summary>
    public class IgniteTupleTests
    {
        [TearDown]
        public void TearDown()
        {
            TestUtils.CheckByteArrayPoolLeak();
        }

        [Test]
        public void TestCreateUpdateRead()
        {
            IIgniteTuple tuple = CreateTuple(new IgniteTuple());
            Assert.AreEqual(0, tuple.FieldCount);

            tuple["foo"] = 1;
            tuple["bar"] = Guid.Empty;

            Assert.AreEqual(2, tuple.FieldCount);

            Assert.AreEqual(1, tuple["foo"]);
            Assert.AreEqual(Guid.Empty, tuple["bar"]);

            Assert.AreEqual(1, tuple[0]);
            Assert.AreEqual(Guid.Empty, tuple[1]);

            Assert.AreEqual("FOO", tuple.GetName(0));
            Assert.AreEqual("BAR", tuple.GetName(1));

            Assert.AreEqual(0, tuple.GetOrdinal("foo"));
            Assert.AreEqual(0, tuple.GetOrdinal("Foo"));
            Assert.AreEqual(0, tuple.GetOrdinal("FOO"));
            Assert.AreEqual(0, tuple.GetOrdinal("\"FOO\""));
            Assert.AreEqual(-1, tuple.GetOrdinal("\"Foo\""));
            Assert.AreEqual(1, tuple.GetOrdinal("bar"));

            tuple[0] = 2;
            tuple["bar"] = "x";
            tuple["qux"] = null;

            Assert.AreEqual(3, tuple.FieldCount);

            Assert.AreEqual(2, tuple["foo"]);
            Assert.AreEqual("x", tuple[1]);
            Assert.IsNull(tuple[2]);
        }

        [Test]
        public void TestGetNullOrEmptyNameThrowsException()
        {
            var tuple = CreateTuple(new IgniteTuple { ["Foo"] = 1 });

            var ex = Assert.Throws<ArgumentException>(() => tuple.GetOrdinal(string.Empty));
            Assert.AreEqual("Column name can not be null or empty.", ex!.Message);

            ex = Assert.Throws<ArgumentException>(() => tuple.GetOrdinal(null!));
            Assert.AreEqual("Column name can not be null or empty.", ex!.Message);

            ex = Assert.Throws<ArgumentException>(() =>
            {
                var unused = tuple[string.Empty];
            });
            Assert.AreEqual("Column name can not be null or empty.", ex!.Message);

            ex = Assert.Throws<ArgumentException>(() =>
            {
                var unused = tuple[null!];
            });
            Assert.AreEqual("Column name can not be null or empty.", ex!.Message);
        }

        [Test]
        public void TestGetNonExistingNameThrowsException()
        {
            var tuple = CreateTuple(new IgniteTuple { ["Foo"] = 1 });

            var ex = Assert.Throws<KeyNotFoundException>(() => { _ = tuple["bar"]; });
            Assert.AreEqual("The given key 'BAR' was not present in the dictionary.", ex!.Message);
        }

        [Test]
        public void TestToStringEmpty()
        {
            var tuple = CreateTuple(new IgniteTuple());
            Assert.AreEqual(GetShortClassName() + " { }", tuple.ToString());
        }

        [Test]
        public void TestToStringOneField()
        {
            var tuple = CreateTuple(new IgniteTuple { ["foo"] = 1 });
            Assert.AreEqual(GetShortClassName() + " { FOO = 1 }", tuple.ToString());
        }

        [Test]
        public void TestToStringTwoFields()
        {
            var tuple = CreateTuple(new IgniteTuple
            {
                ["foo"] = 1,
                ["b"] = "abcd"
            });

            Assert.AreEqual(GetShortClassName() + " { FOO = 1, B = abcd }", tuple.ToString());
        }

        [Test]
        public void TestEquality()
        {
            var guid = Guid.NewGuid();

            var t1 = CreateTuple(new IgniteTuple(2) { ["k"] = 1, ["v"] = "2", ["v2"] = guid });
            var t2 = CreateTuple(new IgniteTuple(3) { ["K"] = 1, ["V"] = "2", ["V2"] = guid });
            var t3 = CreateTuple(new IgniteTuple(4) { ["k"] = 1, ["v"] = null, ["v2"] = guid });
            var t4 = CreateTuple(new IgniteTuple(5) { ["v"] = "2", ["k"] = 1, ["V2"] = guid });
            var t5 = CreateTuple(new IgniteTuple(6) { ["v"] = "2", ["k"] = 1, ["v2"] = guid, ["v3"] = 1 });

            Assert.AreEqual(t1, t2);
            Assert.AreEqual(t2, t1);
            Assert.AreEqual(t1.GetHashCode(), t2.GetHashCode());

            Assert.AreNotEqual(t1, t3);
            Assert.AreNotEqual(t1.GetHashCode(), t3.GetHashCode());

            Assert.AreNotEqual(t2, t3);
            Assert.AreNotEqual(t2.GetHashCode(), t3.GetHashCode());

            Assert.AreEqual(t1, t4);
            Assert.AreEqual(t2, t4);
            Assert.AreEqual(t2.GetHashCode(), t4.GetHashCode());

            Assert.AreNotEqual(t4, t5);
            Assert.AreNotEqual(t4.GetHashCode(), t5.GetHashCode());
        }

        [Test]
        public void TestTupleEqualityDifferentColumnOrder()
        {
            var randomBytes = new byte[100];
            Random.Shared.NextBytes(randomBytes);

            var data = new Dictionary<string, object?>
            {
                { "nil", null },
                { "int", Random.Shared.Next() },
                { "short", (short)Random.Shared.Next() },
                { "sbyte", (sbyte)Random.Shared.Next() },
                { "long", Random.Shared.NextInt64() },
                { "dbl", Random.Shared.NextDouble() },
                { "flt", Random.Shared.NextSingle() },
                { "bytes", randomBytes },
                { "str", "s-" + Random.Shared.Next() },
                { "guid", Guid.NewGuid() },
                { "dt", LocalDateTime.FromDateTime(DateTime.UtcNow) },
                { "bool", Random.Shared.Next() % 2 == 0 },
                { "dec", (decimal)Random.Shared.NextDouble() }
            };

            var tuple1 = CreateTuple(GetRandomizedTuple());
            var tuple2 = CreateTuple(GetRandomizedTuple());

            Assert.AreEqual(tuple1, tuple2);
            Assert.AreEqual(tuple1.GetHashCode(), tuple2.GetHashCode());
            Assert.AreNotEqual(tuple1.ToString(), tuple2.ToString());

            IgniteTuple GetRandomizedTuple() =>
                data
                    .OrderBy(_ => Random.Shared.Next())
                    .Aggregate(new IgniteTuple(), (tuple, pair) =>
                    {
                        var name = Random.Shared.Next() % 2 == 0
                            ? pair.Key.ToUpperInvariant()
                            : pair.Key.ToLowerInvariant();

                        tuple[name] = pair.Value;

                        return tuple;
                    });
        }

        [Test]
        public void TestCustomTupleEquality()
        {
            var tuple = CreateTuple(new IgniteTuple { ["key"] = 42L, ["val"] = "Val1" });
            var customTuple = new CustomTestIgniteTuple();

            Assert.IsTrue(IIgniteTuple.Equals(tuple, customTuple));
            Assert.AreEqual(IIgniteTuple.GetHashCode(tuple), IIgniteTuple.GetHashCode(customTuple));
        }

        protected virtual string GetShortClassName() => nameof(IgniteTuple);

        protected virtual IIgniteTuple CreateTuple(IIgniteTuple source) => source;
    }
}
