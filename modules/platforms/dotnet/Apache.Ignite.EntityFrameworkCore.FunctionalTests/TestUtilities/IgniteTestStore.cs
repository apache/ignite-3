// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

namespace Apache.Ignite.EntityFrameworkCore.FunctionalTests.TestUtilities;

using System.Diagnostics.CodeAnalysis;
using DataCommon;
using Extensions;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.TestUtilities;

public class IgniteTestStore : RelationalTestStore
{
    [SuppressMessage("ReSharper", "VirtualMemberCallInConstructor", Justification = "Reviewed.")]
    public IgniteTestStore(string name, bool shared)
        : base(name, shared)
    {
        ConnectionString = GetIgniteEndpoint();
        Connection = new IgniteConnection(ConnectionString);
    }

    public override DbContextOptionsBuilder AddProviderOptions(DbContextOptionsBuilder builder) =>
        builder.UseIgnite(GetIgniteEndpoint());

    public override void Clean(DbContext context) => DropAllTables().GetAwaiter().GetResult();

    public override TestStore Initialize(
        IServiceProvider serviceProvider,
        Func<DbContext> createContext,
        Action<DbContext>? seed = null,
        Action<DbContext>? clean = null)
    {
        using var context = createContext();
        Clean(context);
        context.Database.EnsureCreated();

        base.Initialize(serviceProvider, createContext, seed, clean);

        return this;
    }

    private static async Task DropAllTables()
    {
        // Drop all tables so that EnsureCreatedAsync works as expected and every test starts with a clean slate.
        using var client = await IgniteClient.StartAsync(new(GetIgniteEndpoint()));

        var tables = await client.Tables.GetTablesAsync();
        var script = string.Join("\n", tables.Select(t => $"DROP TABLE {t.Name}; "));

        if (!string.IsNullOrWhiteSpace(script))
        {
            await client.Sql.ExecuteScriptAsync(script);
        }
    }

    private static string GetIgniteEndpoint() => "localhost:10942";
}
