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

namespace Apache.Ignite.Benchmarks;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Ignite.Sql;
using Ignite.Table;

internal static class Program
{
    private static async Task Main()
    {
        await ManyConnectionsBenchmark.RunAsync();

        using var client = await IgniteClient.StartAsync(new("localhost"));

        ITable? table = await client.Tables.GetTableAsync("Person");
        IRecordView<Person> view = table!.GetRecordView<Person>();

        IQueryable<string> query = view.AsQueryable()
            .Where(p => p.Id > 100)
            .Select(p => p.Name);

        List<string> names = await query.ToListAsync();
    }

    public record Person(int Id, string Name);
}
