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
    using System.Threading.Tasks;
    using Ignite.Table;
    using NUnit.Framework;

    /// <summary>
    /// Tests for POCO view.
    /// </summary>
    public class RecordViewPocoTests : IgniteTestsBase
    {
        [Test]
        public async Task TestUpsertGet()
        {
            await PocoView.UpsertAsync(null, GetPoco(1, "foo"));

            var keyTuple = GetPoco(1);
            var resTuple = (await PocoView.GetAsync(null, keyTuple))!;

            Assert.IsNotNull(resTuple);

            Assert.AreEqual(1L, resTuple.Key);
            Assert.AreEqual("foo", resTuple.Val);

            Assert.IsNull(resTuple.UnmappedStr);
            Assert.AreEqual(default(Guid), resTuple.UnmappedId);
        }
    }
}
