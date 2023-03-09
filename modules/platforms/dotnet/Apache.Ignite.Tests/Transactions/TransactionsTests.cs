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

namespace Apache.Ignite.Tests.Transactions
{
    using System;
    using System.Linq;
    using System.Threading.Tasks;
    using System.Transactions;
    using Ignite.Transactions;
    using NUnit.Framework;
    using Table;
    using TransactionOptions = Ignite.Transactions.TransactionOptions;

    /// <summary>
    /// Tests for <see cref="ITransactions"/> and <see cref="ITransaction"/>.
    /// </summary>
    public class TransactionsTests : IgniteTestsBase
    {
        [TearDown]
        public async Task CleanTable()
        {
            await TupleView.DeleteAllAsync(null, Enumerable.Range(1, 2).Select(x => GetTuple(x)));
        }

        [Test]
        public async Task TestRecordViewBinaryOperations()
        {
            var key = GetTuple(1);
            await TupleView.UpsertAsync(null, GetTuple(1, "1"));

            await using var tx = await Client.Transactions.BeginAsync();
            await TupleView.UpsertAsync(tx, GetTuple(1, "22"));

            Assert.IsFalse(await TupleView.DeleteExactAsync(tx, GetTuple(1, "1")));

            Assert.IsFalse(await TupleView.InsertAsync(tx, GetTuple(1, "111")));
            Assert.AreEqual(GetTuple(1, "22"), (await TupleView.GetAsync(tx, key)).Value);
            Assert.AreEqual(GetTuple(1, "22"), (await TupleView.GetAndUpsertAsync(tx, GetTuple(1, "33"))).Value);
            Assert.AreEqual(GetTuple(1, "33"), (await TupleView.GetAndReplaceAsync(tx, GetTuple(1, "44"))).Value);
            Assert.IsTrue(await TupleView.ReplaceAsync(tx, GetTuple(1, "55")));
            Assert.AreEqual(GetTuple(1, "55"), (await TupleView.GetAndDeleteAsync(tx, key)).Value);
            Assert.IsFalse(await TupleView.DeleteAsync(tx, key));

            await TupleView.UpsertAllAsync(tx, new[] { GetTuple(1, "6"), GetTuple(2, "7") });
            Assert.AreEqual(2, (await TupleView.GetAllAsync(tx, new[] { key, GetTuple(2), GetTuple(3) })).Count);

            var insertAllRes = await TupleView.InsertAllAsync(tx, new[] { GetTuple(1, "8"), GetTuple(3, "9") });
            Assert.AreEqual(GetTuple(1, "6"), (await TupleView.GetAsync(tx, key)).Value);
            Assert.AreEqual(GetTuple(1, "8"), insertAllRes.Single());

            Assert.IsFalse(await TupleView.ReplaceAsync(tx, GetTuple(-1)));
            Assert.IsTrue(await TupleView.ReplaceAsync(tx, GetTuple(1, "10")));
            Assert.AreEqual(GetTuple(1, "10"), (await TupleView.GetAsync(tx, key)).Value);

            Assert.IsFalse(await TupleView.ReplaceAsync(tx, GetTuple(1, "1"), GetTuple(1, "11")));
            Assert.IsTrue(await TupleView.ReplaceAsync(tx, GetTuple(1, "10"), GetTuple(1, "12")));
            Assert.AreEqual(GetTuple(1, "12"), (await TupleView.GetAsync(tx, key)).Value);

            var deleteAllRes = await TupleView.DeleteAllAsync(tx, new[] { GetTuple(3), GetTuple(4) });
            Assert.AreEqual(4, deleteAllRes.Single()[0]);
            Assert.IsFalse((await TupleView.GetAsync(tx, GetTuple(3))).HasValue);

            var deleteAllExactRes = await TupleView.DeleteAllAsync(tx, new[] { GetTuple(1, "12"), GetTuple(5) });
            Assert.AreEqual(5, deleteAllExactRes.Single()[0]);
            Assert.IsFalse((await TupleView.GetAsync(tx, key)).HasValue);

            await tx.RollbackAsync();
            Assert.AreEqual(GetTuple(1, "1"), (await TupleView.GetAsync(null, key)).Value);
        }

        [Test]
        public async Task TestCommitUpdatesData()
        {
            await using var tx = await Client.Transactions.BeginAsync();
            await TupleView.UpsertAsync(tx, GetTuple(1, "2"));
            await tx.CommitAsync();

            var res = await TupleView.GetAsync(null, GetTuple(1));
            Assert.AreEqual("2", res.Value[ValCol]);
        }

        [Test]
        public async Task TestRollbackDoesNotUpdateData()
        {
            await using var tx = await Client.Transactions.BeginAsync();
            await TupleView.UpsertAsync(tx, GetTuple(1, "2"));
            await tx.RollbackAsync();

            var (_, hasValue) = await TupleView.GetAsync(null, GetTuple(1));
            Assert.IsFalse(hasValue);
        }

        [Test]
        public async Task TestDisposeDoesNotUpdateData()
        {
            await using (var tx = await Client.Transactions.BeginAsync())
            {
                await TupleView.UpsertAsync(tx, GetTuple(1, "2"));
                await tx.RollbackAsync();
            }

            var (_, hasValue) = await TupleView.GetAsync(null, GetTuple(1));
            Assert.IsFalse(hasValue);
        }

        [Test]
        public async Task TestCommitAfterRollbackIsIgnored()
        {
            await using var tx = await Client.Transactions.BeginAsync();
            await tx.CommitAsync();

            var ex = Assert.ThrowsAsync<TransactionException>(async () => await tx.RollbackAsync());
            Assert.AreEqual("Transaction is already committed.", ex?.Message);
        }

        [Test]
        public async Task TestRollbackCommitSameTxThrows()
        {
            await using var tx = await Client.Transactions.BeginAsync();
            await tx.RollbackAsync();

            var ex = Assert.ThrowsAsync<TransactionException>(async () => await tx.CommitAsync());
            Assert.AreEqual("Transaction is already rolled back.", ex?.Message);
        }

        [Test]
        public async Task TestMultipleDisposeIsAllowed()
        {
            var tx = await Client.Transactions.BeginAsync();

            await tx.DisposeAsync();
            await tx.DisposeAsync();

            var ex = Assert.ThrowsAsync<TransactionException>(async () => await tx.CommitAsync());
            Assert.AreEqual("Transaction is already rolled back.", ex?.Message);
        }

        [Test]
        public void TestCustomTransactionInterfaceThrows()
        {
            var ex = Assert.ThrowsAsync<TransactionException>(
                async () => await TupleView.UpsertAsync(new CustomTx(), GetTuple(1, "2")));

            StringAssert.StartsWith("Unsupported transaction implementation", ex?.Message);
        }

        [Test]
        public async Task TestClientDisconnectClosesActiveTransactions()
        {
            await using var tx0 = await Client.Transactions.BeginAsync();
            await TupleView.UpsertAsync(null, GetTuple(1, "1"));

            using (var client2 = await IgniteClient.StartAsync(GetConfig()))
            {
                var table = await client2.Tables.GetTableAsync(TableName);
                var tx1 = await client2.Transactions.BeginAsync();

                await table!.RecordBinaryView.UpsertAsync(tx1, GetTuple(1, "2"));
            }

            // The code above is intentionally written in a way that we have no guarantee that the lock taken by tx1 is already released,
            // as client2 is closed without rolling back tx1, forcing Ignite server to rollback tx1 in background. So we should check the
            // value using transaction that is older than tx1, to make sure that the conflict on key 1 between tx0 and tx1 will not lead
            // to exception.
            Assert.AreEqual("1", (await TupleView.GetAsync(tx0, GetTuple(1))).Value[ValCol]);
        }

        [Test]
        public async Task TestTransactionFromAnotherClientThrows()
        {
            using var client2 = await IgniteClient.StartAsync(GetConfig());
            await using var tx = await client2.Transactions.BeginAsync();

            var ex = Assert.ThrowsAsync<IgniteClientException>(async () => await TupleView.UpsertAsync(tx, GetTuple(1, "2")));
            Assert.AreEqual("Specified transaction belongs to a different IgniteClient instance.", ex!.Message);
        }

        [Test]
        public async Task TestReadOnlyTxSeesOldDataAfterUpdate()
        {
            var key = Random.Shared.NextInt64(1000, long.MaxValue);
            var keyPoco = new Poco { Key = key };

            await PocoView.UpsertAsync(null, new Poco { Key = key, Val = "11" });

            await using var tx = await Client.Transactions.BeginAsync(new TransactionOptions { ReadOnly = true });
            Assert.AreEqual("11", (await PocoView.GetAsync(tx, keyPoco)).Value.Val);

            // Update data in a different tx.
            await using (var tx2 = await Client.Transactions.BeginAsync())
            {
                await PocoView.UpsertAsync(null, new Poco { Key = key, Val = "22" });
                await tx2.CommitAsync();
            }

            // Old tx sees old data.
            Assert.AreEqual("11", (await PocoView.GetAsync(tx, keyPoco)).Value.Val);

            // New tx sees new data
            await using var tx3 = await Client.Transactions.BeginAsync(new TransactionOptions { ReadOnly = true });
            Assert.AreEqual("22", (await PocoView.GetAsync(tx3, keyPoco)).Value.Val);
        }

        [Test]
        public async Task TestUpdateInReadOnlyTxThrows()
        {
            await using var tx = await Client.Transactions.BeginAsync(new TransactionOptions { ReadOnly = true });
            var ex = Assert.ThrowsAsync<Tx.TransactionException>(async () => await TupleView.UpsertAsync(tx, GetTuple(1, "1")));

            Assert.AreEqual(ErrorGroups.Transactions.TxFailedReadWriteOperation, ex!.Code, ex.Message);
            StringAssert.Contains("Failed to enlist read-write operation into read-only transaction", ex.Message);
        }

        [Test]
        public async Task TestCommitRollbackReadOnlyTxDoesNothing([Values(true, false)] bool commit)
        {
            await using var tx = await Client.Transactions.BeginAsync(new TransactionOptions { ReadOnly = true });
            var res = await PocoView.GetAsync(tx, new Poco { Key = 123 });

            if (commit)
            {
                await tx.CommitAsync();
            }
            else
            {
                await tx.RollbackAsync();
            }

            Assert.IsFalse(res.HasValue);
        }

        [Test]
        public async Task TestReadOnlyTxAttributes()
        {
            await using var tx = await Client.Transactions.BeginAsync(new TransactionOptions { ReadOnly = true });

            Assert.IsTrue(tx.IsReadOnly);
        }

        [Test]
        public async Task TestReadWriteTxAttributes()
        {
            await using var tx = await Client.Transactions.BeginAsync();

            Assert.IsFalse(tx.IsReadOnly);
        }

        private class CustomTx : ITransaction
        {
            public bool IsReadOnly => false;

            public ValueTask DisposeAsync()
            {
                return new ValueTask(Task.CompletedTask);
            }

            public Task CommitAsync()
            {
                return Task.CompletedTask;
            }

            public Task RollbackAsync()
            {
                return Task.CompletedTask;
            }
        }
    }
}
