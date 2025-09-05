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

namespace Apache.Ignite.Tests.Sql;

using System.Threading.Tasks;
using Ignite.Sql;
using Ignite.Transactions;
using NUnit.Framework;

/// <summary>
/// Tests for <see cref="IgniteDbTransaction"/>.
/// </summary>
public class IgniteDbTransactionTests
{
    [Test]
    public void TestCommit()
    {
        var tx = new TestIgniteTx();
        var dbTx = new IgniteDbTransaction(tx, System.Data.IsolationLevel.ReadCommitted, null!);

        Assert.AreEqual(System.Data.IsolationLevel.ReadCommitted, dbTx.IsolationLevel);
        Assert.AreSame(tx, dbTx.IgniteTransaction);

        dbTx.Commit();

        Assert.IsTrue(tx.IsCommitted);
        Assert.IsFalse(tx.IsRolledback);
        Assert.IsFalse(tx.IsDisposed);
    }

    private class TestIgniteTx : ITransaction
    {
        public bool IsDisposed { get; set; }

        public bool IsCommitted { get; set; }

        public bool IsRolledback { get; set; }

        public bool IsReadOnly => false;

        public ValueTask DisposeAsync()
        {
            IsDisposed = true;
            return ValueTask.CompletedTask;
        }

        public void Dispose()
        {
            IsDisposed = true;
        }

        public Task CommitAsync()
        {
            IsCommitted = true;
            return Task.CompletedTask;
        }

        public Task RollbackAsync()
        {
            IsRolledback = true;
            return Task.CompletedTask;
        }
    }
}


