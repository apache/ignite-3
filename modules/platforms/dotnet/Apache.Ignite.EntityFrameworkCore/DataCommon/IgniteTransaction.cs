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

namespace Apache.Ignite.EntityFrameworkCore.DataCommon;

using System.Data;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;
using Transactions;

public class IgniteTransaction : DbTransaction
{
    public IgniteTransaction(ITransaction tx, IsolationLevel isolationLevel, DbConnection connection)
    {
        InternalTransaction = tx;
        IsolationLevel = isolationLevel;
        DbConnection = connection;
    }

    public override IsolationLevel IsolationLevel { get; }

    public override bool SupportsSavepoints => false;

    internal ITransaction InternalTransaction { get; }

    protected override DbConnection DbConnection { get; }

    public override void Commit() => CommitAsync(CancellationToken.None).GetAwaiter().GetResult();

    public override void Rollback() => RollbackAsync(null!, CancellationToken.None).GetAwaiter().GetResult();

    public override async Task CommitAsync(CancellationToken cancellationToken = default) =>
        await InternalTransaction.CommitAsync();

    public override async Task RollbackAsync(string savepointName, CancellationToken cancellationToken = default) =>
        await InternalTransaction.RollbackAsync();


}
