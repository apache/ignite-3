namespace Apache.Ignite.EntityFrameworkCore.DataCommon;

using System;
using System.Data;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;
using Apache.Ignite.Transactions;

public class IgniteTransaction : DbTransaction
{
    private readonly ITransaction _tx;

    public IgniteTransaction(ITransaction tx, IsolationLevel isolationLevel, DbConnection connection)
    {
        _tx = tx;
        IsolationLevel = isolationLevel;
        DbConnection = connection;
    }

    public override void Commit()
    {
        throw new NotImplementedException();
    }

    public override void Rollback()
    {
        throw new NotImplementedException();
    }

    public override async Task CommitAsync(CancellationToken cancellationToken) =>
        await _tx.CommitAsync();

    public override async Task RollbackAsync(string savepointName, CancellationToken cancellationToken) =>
        await _tx.RollbackAsync();

    protected override DbConnection DbConnection { get; }

    public override IsolationLevel IsolationLevel { get; }
}
