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

namespace Apache.Ignite.EntityFrameworkCore.Infrastructure.Internal;

using System;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;

public class IgniteModelValidator : RelationalModelValidator
{
    public IgniteModelValidator(
        ModelValidatorDependencies dependencies,
        RelationalModelValidatorDependencies relationalDependencies)
        : base(dependencies, relationalDependencies)
    {
    }

    public override void Validate(IModel model, IDiagnosticsLogger<DbLoggerCategory.Model.Validation> logger)
    {
        base.Validate(model, logger);

        ValidateNoSchemas(model, logger);
        ValidateNoSequences(model, logger);
        ValidateNoStoredProcedures(model, logger);
    }

    protected virtual void ValidateNoSchemas(
        IModel model,
        IDiagnosticsLogger<DbLoggerCategory.Model.Validation> logger)
    {
        // TODO
    }

    protected virtual void ValidateNoSequences(
        IModel model,
        IDiagnosticsLogger<DbLoggerCategory.Model.Validation> logger)
    {
        // TODO
    }

    protected virtual void ValidateNoStoredProcedures(
        IModel model,
        IDiagnosticsLogger<DbLoggerCategory.Model.Validation> logger)
    {
        foreach (var entityType in model.GetEntityTypes())
        {
            if (entityType.GetInsertStoredProcedure() is not null
                || entityType.GetUpdateStoredProcedure() is not null
                || entityType.GetDeleteStoredProcedure() is not null)
            {
                throw new InvalidOperationException("Stored procedures are not supported in Ignite: " + entityType.DisplayName());
            }
        }
    }
}
