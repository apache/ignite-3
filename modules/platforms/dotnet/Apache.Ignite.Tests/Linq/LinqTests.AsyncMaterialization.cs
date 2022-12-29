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

namespace Apache.Ignite.Tests.Linq;

using System.Linq;
using System.Threading.Tasks;
using Ignite.Sql;
using NUnit.Framework;

/// <summary>
/// Linq async materialization tests (retrieving results in async manner, such as CountAsync or ToListAsync).
/// </summary>
public partial class LinqTests
{
    [Test]
    public async Task TestAnyAsync()
    {
        // TODO: AnyAsync with predicate
        Assert.IsTrue(await PocoView.AsQueryable().AnyAsync());
        Assert.IsFalse(await PocoView.AsQueryable().Where(x => x.Key > 1000).AnyAsync());
    }
}
