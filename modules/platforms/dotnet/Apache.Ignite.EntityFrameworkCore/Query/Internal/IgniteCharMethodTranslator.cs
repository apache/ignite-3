
namespace Apache.Ignite.EntityFrameworkCore.Query.Internal;

using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;

public class IgniteCharMethodTranslator : IMethodCallTranslator
{
    private static readonly Dictionary<MethodInfo, string> SupportedMethods = new()
    {
        { typeof(char).GetRuntimeMethod(nameof(char.ToLower), new[] { typeof(char) })!, "lower" },
        { typeof(char).GetRuntimeMethod(nameof(char.ToUpper), new[] { typeof(char) })!, "upper" }
    };

    private readonly ISqlExpressionFactory _sqlExpressionFactory;

    public IgniteCharMethodTranslator(ISqlExpressionFactory sqlExpressionFactory)
    {
        _sqlExpressionFactory = sqlExpressionFactory;
    }

    public virtual SqlExpression? Translate(
        SqlExpression? instance,
        MethodInfo method,
        IReadOnlyList<SqlExpression> arguments,
        IDiagnosticsLogger<DbLoggerCategory.Query> logger)
    {
        if (SupportedMethods.TryGetValue(method, out var sqlFunctionName))
        {
            return _sqlExpressionFactory.Function(
                sqlFunctionName,
                arguments,
                nullable: true,
                argumentsPropagateNullability: arguments.Select(_ => true).ToList(),
                method.ReturnType,
                arguments[0].TypeMapping);
        }

        return null;
    }
}
