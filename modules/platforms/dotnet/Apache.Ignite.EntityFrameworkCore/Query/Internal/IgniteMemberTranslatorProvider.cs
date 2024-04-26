
namespace Apache.Ignite.EntityFrameworkCore.Query.Internal;

using Microsoft.EntityFrameworkCore.Query;

public class IgniteMemberTranslatorProvider : RelationalMemberTranslatorProvider
{
    public IgniteMemberTranslatorProvider(RelationalMemberTranslatorProviderDependencies dependencies)
        : base(dependencies)
    {
        var sqlExpressionFactory = (IgniteSqlExpressionFactory)dependencies.SqlExpressionFactory;

        AddTranslators(
            new IMemberTranslator[]
            {
                new IgniteDateTimeMemberTranslator(sqlExpressionFactory),
                new IgniteStringLengthTranslator(sqlExpressionFactory),
                new IgniteDateOnlyMemberTranslator(sqlExpressionFactory)
            });
    }
}
