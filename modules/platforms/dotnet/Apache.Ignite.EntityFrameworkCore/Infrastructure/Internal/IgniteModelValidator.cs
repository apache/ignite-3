
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
