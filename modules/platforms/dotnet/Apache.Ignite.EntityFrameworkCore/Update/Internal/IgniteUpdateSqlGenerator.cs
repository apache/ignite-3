
namespace Apache.Ignite.EntityFrameworkCore.Update.Internal;

using System;
using System.Text;
using Common;
using Microsoft.EntityFrameworkCore.Update;

public class IgniteUpdateSqlGenerator : UpdateAndSelectSqlGenerator
{
    public IgniteUpdateSqlGenerator(UpdateSqlGeneratorDependencies dependencies)
        : base(dependencies)
    {
    }

    protected override void AppendIdentityWhereCondition(StringBuilder commandStringBuilder, IColumnModification columnModification)
    {
        Check.NotNull(commandStringBuilder, nameof(commandStringBuilder));
        Check.NotNull(columnModification, nameof(columnModification));

        // SqlGenerationHelper.DelimitIdentifier(commandStringBuilder, "rowid");
        // commandStringBuilder.Append(" = ").Append("last_insert_rowid()");
        throw new NotSupportedException("Ignite does not support identity columns.");
    }

    protected override ResultSetMapping AppendSelectAffectedCountCommand(
        StringBuilder commandStringBuilder,
        string name,
        string? schema,
        int commandPosition)
    {
        // Ignite-specific: no-op, affected rows in ResultSet.
        return ResultSetMapping.NoResults;
    }

    /// <summary>
    ///     Appends a <c>WHERE</c> condition checking rows affected.
    /// </summary>
    /// <param name="commandStringBuilder">The builder to which the SQL should be appended.</param>
    /// <param name="expectedRowsAffected">The expected number of rows affected.</param>
    protected override void AppendRowsAffectedWhereCondition(StringBuilder commandStringBuilder, int expectedRowsAffected)
    {
        Check.NotNull(commandStringBuilder, nameof(commandStringBuilder));

        throw new NotSupportedException("Ignite does not support affected rows check.");
    }

    public override string GenerateNextSequenceValueOperation(string name, string? schema)
        => throw new NotSupportedException(IgniteStrings.SequencesNotSupported);
}
