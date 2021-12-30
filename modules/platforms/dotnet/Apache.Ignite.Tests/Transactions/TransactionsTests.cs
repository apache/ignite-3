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
    using System.Threading.Tasks;
    using System.Transactions;
    using Ignite.Transactions;
    using NUnit.Framework;

    /// <summary>
    /// Tests for <see cref="ITransactions"/> and <see cref="ITransaction"/>.
    /// </summary>
    public class TransactionsTests : IgniteTestsBase
    {
        [Test]
        public async Task TestRecordViewBinaryOperations()
        {
            await Table.UpsertAsync(null, GetTuple(1, "1"));

            await using var tx = await Client.Transactions.BeginAsync();

            // TODO
            // Assert.IsFalse(await Table.DeleteExactAsync(tx, GetTuple(1, "1")));
            // assertFalse(recordView.insert(tx, kv(1, "111")));
            // assertEquals(kv(1, "22"), recordView.get(tx, key));
            // assertEquals(kv(1, "22"), recordView.getAndUpsert(tx, kv(1, "33")));
            // assertEquals(kv(1, "33"), recordView.getAndReplace(tx, kv(1, "44")));
            // assertTrue(recordView.replace(tx, kv(1, "55")));
            // assertEquals(kv(1, "55"), recordView.getAndDelete(tx, key));
            // assertFalse(recordView.delete(tx, key));
            //
            // recordView.upsertAll(tx, List.of(kv(1, "6"), kv(2, "7")));
            // assertEquals(2, recordView.getAll(tx, List.of(key, key(2), key(3))).size());
            //
            // tx.rollback();
            // assertEquals(kv(1, "1"), recordView.get(null, key));
        }

        [Test]
        public async Task TestCommitUpdatesData()
        {
            await using var tx = await Client.Transactions.BeginAsync();
            await Table.UpsertAsync(tx, GetTuple(1, "2"));
            await tx.CommitAsync();

            var res = await Table.GetAsync(null, GetTuple(1));
            Assert.AreEqual("2", res![ValCol]);
        }

        [Test]
        public async Task TestRollbackDoesNotUpdateData()
        {
            await using var tx = await Client.Transactions.BeginAsync();
            await Table.UpsertAsync(tx, GetTuple(1, "2"));
            await tx.RollbackAsync();

            var res = await Table.GetAsync(null, GetTuple(1));
            Assert.IsNull(res);
        }

        [Test]
        public async Task TestDisposeDoesNotUpdateData()
        {
            await using (var tx = await Client.Transactions.BeginAsync())
            {
                await Table.UpsertAsync(tx, GetTuple(1, "2"));
                await tx.RollbackAsync();
            }

            var res = await Table.GetAsync(null, GetTuple(1));
            Assert.IsNull(res);
        }

        [Test]
        public async Task TestCommitRollbackSameTxThrows()
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
                async () => await Table.UpsertAsync(new CustomTx(), GetTuple(1, "2")));

            StringAssert.StartsWith("Unsupported transaction implementation", ex?.Message);
        }

        private class CustomTx : ITransaction
        {
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
