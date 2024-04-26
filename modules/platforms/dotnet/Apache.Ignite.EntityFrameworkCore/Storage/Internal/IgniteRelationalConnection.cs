namespace Apache.Ignite.EntityFrameworkCore.Storage.Internal;

using System.Data.Common;
using System.Linq;
using DataCommon;
using Extensions.Internal;
using Infrastructure.Internal;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Storage;

public class IgniteRelationalConnection : RelationalConnection, IIgniteRelationalConnection
{
    private readonly IDiagnosticsLogger<DbLoggerCategory.Infrastructure> _logger;
    private readonly int? _commandTimeout;

    public IgniteRelationalConnection(
        RelationalConnectionDependencies dependencies,
        IDiagnosticsLogger<DbLoggerCategory.Infrastructure> logger)
        : base(dependencies)
    {
        _logger = logger;

        var optionsExtension = dependencies.ContextOptions.Extensions.OfType<IgniteOptionsExtension>().FirstOrDefault();
        if (optionsExtension != null)
        {
            var relationalOptions = RelationalOptionsExtension.Extract(dependencies.ContextOptions);
            _commandTimeout = relationalOptions.CommandTimeout;

            if (relationalOptions.Connection != null)
            {
                InitializeDbConnection(relationalOptions.Connection);
            }
        }
    }

    protected override DbConnection CreateDbConnection()
    {
        var connection = new IgniteConnection(GetValidatedConnectionString());
        InitializeDbConnection(connection);

        return connection;
    }

    private void InitializeDbConnection(DbConnection connection)
    {
        if (connection is IgniteConnection igniteConn)
        {
            if (_commandTimeout.HasValue)
            {
                igniteConn.DefaultTimeout = _commandTimeout.Value;
            }

        }
        else
        {
            _logger.UnexpectedConnectionTypeWarning(connection.GetType());
        }
    }
}
