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

namespace Apache.Ignite.EntityFrameworkCore.FunctionalTests.TestModels.Northwind;

using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.TestModels.Northwind;

public class NorthwindIgniteContext : NorthwindRelationalContext
{
    public NorthwindIgniteContext(DbContextOptions options)
        : base(options)
    {
    }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        base.OnModelCreating(modelBuilder);

        // Add primary keys - Ignite does not support PK-less entities.
        AddId<CustomerQuery>(modelBuilder);
        AddId<CustomerQueryWithQueryFilter>(modelBuilder);
        AddId<ProductQuery>(modelBuilder);
    }

    private static void AddId<T>(ModelBuilder modelBuilder)
        where T : class
    {
        var entity = modelBuilder.Entity<T>();

        entity.Property<Guid>("id")
            .HasColumnType("uuid")
            .HasDefaultValueSql("rand_uuid");

        entity.HasKey("id");
    }
}
