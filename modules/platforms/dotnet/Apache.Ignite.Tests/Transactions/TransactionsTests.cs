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
    using Ignite.Table;
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
            // TODO
            await Task.Delay(1);
        }

        [Test]
        public async Task TestCommitUpdatesData()
        {
            await using var tx = await Client.Transactions.BeginAsync();

            // TODO: Pass tx.
            await Table.UpsertAsync(tx, new IgniteTuple { [KeyCol] = 1, [ValCol] = "2" });

            await tx.CommitAsync();

            var res = await Table.GetAsync(new IgniteTuple { [KeyCol] = 1 });
            Assert.AreEqual("2", res![ValCol]);
        }

        [Test]
        public async Task TestRollbackDoesNotUpdateData()
        {
            // TODO
            await Task.Delay(1);
        }

        [Test]
        public async Task TestDisposeDoesNotUpdateData()
        {
            // TODO
            await Task.Delay(1);
        }

        [Test]
        public async Task TestAccessLockedKeyTimesOut()
        {
            // TODO
            await Task.Delay(1);
        }

        [Test]
        public async Task TestCommitRollbackSameTxThrows()
        {
            // TODO
            await Task.Delay(1);
        }

        [Test]
        public async Task TestRollbackCommitSameTxThrows()
        {
            // TODO
            await Task.Delay(1);
        }

        [Test]
        public async Task TestMultipleDisposeIsAllowed()
        {
            // TODO
            await Task.Delay(1);
        }

        [Test]
        public async Task TestCustomTransactionInterfaceThrows()
        {
            // TODO
            await Task.Delay(1);
        }
    }
}
