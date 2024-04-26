namespace Apache.Ignite.EntityFrameworkCore.Query.Internal;

using Microsoft.EntityFrameworkCore.Query;

public class IgniteMethodCallTranslatorProvider : RelationalMethodCallTranslatorProvider
{
    public IgniteMethodCallTranslatorProvider(RelationalMethodCallTranslatorProviderDependencies dependencies)
        : base(dependencies)
    {
        var sqlExpressionFactory = (IgniteSqlExpressionFactory)dependencies.SqlExpressionFactory;

        AddTranslators(
            new IMethodCallTranslator[]
            {
                new IgniteCharMethodTranslator(sqlExpressionFactory),
                new IgniteDateOnlyMethodTranslator(sqlExpressionFactory),
                new IgniteDateTimeMethodTranslator(sqlExpressionFactory),
                new IgniteMathTranslator(sqlExpressionFactory),
                new IgniteObjectToStringTranslator(sqlExpressionFactory),
                new IgniteStringMethodTranslator(sqlExpressionFactory),
            });
    }
}
