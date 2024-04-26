
namespace Apache.Ignite.EntityFrameworkCore.Storage.Internal;

using System.Diagnostics;
using DataCommon;
using Microsoft.EntityFrameworkCore.Storage;

public class IgniteDatabaseCreator : RelationalDatabaseCreator
{
    private readonly IIgniteRelationalConnection _connection;

    public IgniteDatabaseCreator(
        RelationalDatabaseCreatorDependencies dependencies,
        IIgniteRelationalConnection connection)
        : base(dependencies)
    {
        _connection = connection;
    }

    public override void Create()
    {
        // No-op.
        // "Database" always exists in Ignite.
    }

    public override bool Exists()
    {
        // "Database" always exists in Ignite.
        return true;
    }

    public override bool HasTables()
    {
        var conn = (IgniteConnection)_connection.DbConnection;
        conn.Open();

        var igniteClient = conn.Client;
        Debug.Assert(igniteClient != null, "igniteClient != null");

        var tables = igniteClient.Tables.GetTablesAsync().GetAwaiter().GetResult();
        return tables.Count > 0;
    }

    public override void Delete()
    {
        // No-op.
    }
}
