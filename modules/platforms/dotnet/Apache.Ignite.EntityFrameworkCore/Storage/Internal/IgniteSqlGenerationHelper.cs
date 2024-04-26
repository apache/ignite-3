
namespace Apache.Ignite.EntityFrameworkCore.Storage.Internal;

using System.Text;
using Microsoft.EntityFrameworkCore.Storage;

public class IgniteSqlGenerationHelper : RelationalSqlGenerationHelper
{
    public IgniteSqlGenerationHelper(RelationalSqlGenerationHelperDependencies dependencies)
        : base(dependencies)
    {
    }

    public override string StartTransactionStatement
        => "BEGIN TRANSACTION" + StatementTerminator;

    public override string DelimitIdentifier(string name, string? schema)
        => base.DelimitIdentifier(name);

    public override void DelimitIdentifier(StringBuilder builder, string name, string? schema)
        => base.DelimitIdentifier(builder, name);

    public override string GenerateParameterName(string name) => "?";

    public override void GenerateParameterName(StringBuilder builder, string name) => builder.Append('?');

    public override string GenerateParameterNamePlaceholder(string name)
    {
        // TODO: ??
        return base.GenerateParameterNamePlaceholder(name);
    }

    public override void GenerateParameterNamePlaceholder(StringBuilder builder, string name)
    {
        // TODO: ??
        base.GenerateParameterNamePlaceholder(builder, name);
    }
}
