namespace Apache.Ignite.EntityFrameworkCore.DataCommon;

using System;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Apache.Ignite.Sql;
using Microsoft.EntityFrameworkCore.Diagnostics;

public class IgniteCommand : DbCommand
{
    private IgniteParameterCollection? _parameters = null;

    public override void Cancel()
    {
        // No-op.
    }

    public override int ExecuteNonQuery()
    {
        return ExecuteNonQueryAsync(CancellationToken.None).GetAwaiter().GetResult();
    }

    protected override async Task<DbDataReader> ExecuteDbDataReaderAsync(CommandBehavior behavior, CancellationToken cancellationToken)
    {
        if (CommandSource == CommandSource.SaveChanges)
        {
            // Ignite-specific: SaveChangesDataReader is used to return the number of rows affected.
            var rowsAffected = await ExecuteNonQueryAsync(cancellationToken);

            return new IgniteSaveChangesDataReader(rowsAffected);
        }

        var args = _parameters?.ToObjectArray() ?? Array.Empty<object>();

        // TODO: Remove debug output.
        Console.WriteLine($"IgniteCommand.ExecuteDbDataReaderAsync [statement={CommandText}, parameters={string.Join(", ", args)}]");

        // TODO: Propagate transaction somehow.
        return await Sql.ExecuteReaderAsync(
            null,
            CommandText,
            args);
    }

    public override async Task<int> ExecuteNonQueryAsync(CancellationToken cancellationToken)
    {
        var args = _parameters?.ToObjectArray() ?? Array.Empty<object>();

        // TODO: Remove debug output.
        Console.WriteLine($"IgniteCommand.ExecuteNonQueryAsync [statement={CommandText}, parameters={string.Join(", ", args)}]");

        // TODO: Propagate transaction somehow.
        await using IResultSet<object> resultSet = await Sql.ExecuteAsync<object>(
            transaction: null,
            CommandText,
            args);

        Debug.Assert(!resultSet.HasRowSet, "!resultSet.HasRowSet");

        return (int)resultSet.AffectedRows;
    }

    public override Task<object> ExecuteScalarAsync(CancellationToken cancellationToken)
    {
        throw new NotImplementedException();
    }

    public override object ExecuteScalar() => ExecuteScalarAsync().GetAwaiter().GetResult();

    public override void Prepare()
    {
        throw new NotImplementedException();
    }

    public override string CommandText { get; set; }

    public override int CommandTimeout { get; set; }

    public override CommandType CommandType { get; set; }

    public override UpdateRowSource UpdatedRowSource { get; set; }

    protected override DbConnection DbConnection { get; set; }

    protected override DbParameterCollection DbParameterCollection =>
        _parameters ??= new IgniteParameterCollection();

    protected override DbTransaction DbTransaction { get; set; }

    public override bool DesignTimeVisible { get; set; }

    public CommandSource CommandSource { get; set; }

    protected override DbParameter CreateDbParameter() => new IgniteParameter();

    protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
    {
        throw new NotImplementedException();
    }

    private IIgniteClient IgniteClient => ((IgniteConnection)DbConnection).Client;

    private ISql Sql => IgniteClient.Sql;
}
