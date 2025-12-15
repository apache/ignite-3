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

namespace Apache.Ignite.Tests.Aot.Transactions;

using Common.Table;
using JetBrains.Annotations;

public class TransactionsTests(IIgniteClient client)
{
    [UsedImplicitly]
    public async Task TestCommit()
    {
        var table = await client.Tables.GetTableAsync(TestTables.TableName);
        var view = table!.GetRecordView(new PocoMapper());

        await using var tx = await client.Transactions.BeginAsync();

        var poco = new Poco { Key = 8000, Val = "tx-commit-test" };
        await view.UpsertAsync(tx, poco);

        // Before commit.
        var res1 = await view.GetAsync(tx, new Poco { Key = 8000 });
        Assert.AreEqual("tx-commit-test", res1.Value.Val);

        await tx.CommitAsync();

        // After commit.
        var res2 = await view.GetAsync(null, new Poco { Key = 8000 });
        Assert.AreEqual("tx-commit-test", res2.Value.Val);
    }

    [UsedImplicitly]
    public async Task TestRollback()
    {
        var table = await client.Tables.GetTableAsync(TestTables.TableName);
        var view = table!.GetRecordView(new PocoMapper());

        await using var tx = await client.Transactions.BeginAsync();

        var poco = new Poco { Key = 8001, Val = "tx-rollback-test" };
        await view.UpsertAsync(tx, poco);

        // Before rollback.
        var res1 = await view.GetAsync(tx, new Poco { Key = 8001 });
        Assert.AreEqual("tx-rollback-test", res1.Value.Val);

        await tx.RollbackAsync();

        // After rollback.
        var res2 = await view.GetAsync(null, new Poco { Key = 8001 });
        Assert.AreEqual(false, res2.HasValue);
    }
}
