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

using Extensions;
using Microsoft.EntityFrameworkCore.TestUtilities;
using Microsoft.Extensions.DependencyInjection;

public class IgniteTestStoreFactory : RelationalTestStoreFactory
{
    protected IgniteTestStoreFactory()
    {
        // No-op.
    }

    public static IgniteTestStoreFactory Instance { get; } = new();

    public override TestStore Create(string storeName) => new IgniteTestStore(storeName, shared: false);

    public override TestStore GetOrCreate(string storeName) => Create(storeName);

    public override IServiceCollection AddProviderServices(IServiceCollection serviceCollection) =>
        serviceCollection.AddEntityFrameworkIgnite();
}
