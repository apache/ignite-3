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
