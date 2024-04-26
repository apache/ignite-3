namespace Apache.Ignite.EntityFrameworkCore.Storage.Internal;

using System;
using System.Collections.Generic;
using System.Data.Common;
using DataCommon;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Storage;

public class IgniteRelationalCommand : RelationalCommand
{
    public IgniteRelationalCommand(RelationalCommandBuilderDependencies dependencies, string commandText, IReadOnlyList<IRelationalParameter> parameters)
        : base(dependencies, commandText, parameters)
    {
    }

    public override DbCommand CreateDbCommand(RelationalCommandParameterObject parameterObject, Guid commandId, DbCommandMethod commandMethod)
    {
        var cmd = (IgniteCommand)base.CreateDbCommand(parameterObject, commandId, commandMethod);

        cmd.CommandSource = parameterObject.CommandSource;

        return cmd;
    }
}
