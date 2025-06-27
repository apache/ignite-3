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

namespace Apache.Ignite.EntityFrameworkCore.Tests;

using Extensions;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using NUnit.Framework;

public class BasicTest
{
    [SetUp]
    public async Task SetUp()
    {
        using var client = await IgniteClient.StartAsync(new(GetIgniteEndpoint()));

        var tables = await client.Tables.GetTablesAsync();

        foreach (var table in tables)
        {
            await client.Sql.ExecuteAsync(null, $"DROP TABLE \"{table.Name}\"");
        }
    }

    [Test]
    public async Task TestIgniteEfCore()
    {
        await using var ctx = CreateDbContext();

        await ctx.Database.EnsureCreatedAsync();

        var book = new Book(Guid.NewGuid(), "Nineteen Eighty-Four", 1984, Guid.NewGuid())
        {
            Author = new Author(Guid.NewGuid(), "George", "Orwell")
        };
        ctx.Books.Add(book);
        await ctx.SaveChangesAsync();
        ctx.ChangeTracker.Clear();

        var query = ctx.Books
            .AsNoTracking()
            .Include(x => x.Author)
            .Where(b => b.Year > 1900);

        var books = await query.ToListAsync();

        Assert.AreEqual(1, books.Count);
        Assert.AreEqual(book.Name, books[0].Name);
        Assert.AreEqual(book.Author.FirstName, books[0].Author.FirstName);
        Assert.AreEqual(book.Author.LastName, books[0].Author.LastName);
        Assert.AreEqual(book.Year, books[0].Year);
        Assert.AreEqual(book.Id, books[0].Id);

        var expectedSql =
            """
            SELECT "b"."Id", "b"."AuthorId", "b"."Name", "b"."Year", "a"."Id", "a"."FirstName", "a"."LastName"
            FROM "Books" AS "b"
            INNER JOIN "Authors" AS "a" ON "b"."AuthorId" = "a"."Id"
            WHERE "b"."Year" > 1900
            """;

        var queryString = query.ToQueryString();
        Assert.AreEqual(expectedSql, queryString);
    }

    private static TestDbContext CreateDbContext()
    {
        var contextOptionsBuilder = new DbContextOptionsBuilder<TestDbContext>(
            new DbContextOptions<TestDbContext>(new Dictionary<Type, IDbContextOptionsExtension>()));

        contextOptionsBuilder.UseIgnite(GetIgniteEndpoint());

        return new TestDbContext(contextOptionsBuilder.Options);
    }

    private static string GetIgniteEndpoint() => "localhost:10942";
}
