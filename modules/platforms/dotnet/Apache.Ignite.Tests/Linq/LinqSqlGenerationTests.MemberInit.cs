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

using System.Collections.Generic;
using System.Linq;
using Internal.Linq;
using NUnit.Framework;
using Table;

/// <summary>
/// Tests MemberInit.
/// </summary>
public partial class LinqSqlGenerationTests
{
    [Test]
    public void Do() => AssertSql(
        "select _T0.KEY as KEY, _T0.VAL as VALUE from PUBLIC.tbl1 as _T0",
        q => q.Select(p => new CustomProjection {Key = p.Key, Value = p.Val}).ToList());

    [Test]
    public void Do1() => AssertSql(
        "select _T0.KEY, _T0.VAL from PUBLIC.tbl1 as _T0",
        q => q.Select(p => new CustomProjectionRecord(p.Key, p.Val)).ToList());

    private class CustomProjection
    {
        public long Key { get; set; }

        public string? Value { get; set; }
    }

    private record CustomProjectionRecord(long Key, string? Val);
}
