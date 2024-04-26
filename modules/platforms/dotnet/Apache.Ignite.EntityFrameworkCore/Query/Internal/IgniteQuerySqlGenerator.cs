
namespace Apache.Ignite.EntityFrameworkCore.Query.Internal;

using System;
using System.Linq.Expressions;
using Common;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;

public class IgniteQuerySqlGenerator : QuerySqlGenerator
{
    public IgniteQuerySqlGenerator(QuerySqlGeneratorDependencies dependencies)
        : base(dependencies)
    {
    }

    protected override string GetOperator(SqlBinaryExpression binaryExpression)
        => binaryExpression.OperatorType == ExpressionType.Add
            && binaryExpression.Type == typeof(string)
                ? " || "
                : base.GetOperator(binaryExpression);

    protected override void GenerateLimitOffset(SelectExpression selectExpression)
    {
        if (selectExpression.Limit != null
            || selectExpression.Offset != null)
        {
            Sql.AppendLine()
                .Append("LIMIT ");

            Visit(
                selectExpression.Limit
                ?? new SqlConstantExpression(Expression.Constant(-1), selectExpression.Offset!.TypeMapping));

            if (selectExpression.Offset != null)
            {
                Sql.Append(" OFFSET ");

                Visit(selectExpression.Offset);
            }
        }
    }

    protected override void GenerateSetOperationOperand(SetOperationBase setOperation, SelectExpression operand)
        => Visit(operand);

    protected override Expression VisitSqlUnary(SqlUnaryExpression sqlUnaryExpression)
    {
        switch (sqlUnaryExpression.OperatorType)
        {
            case ExpressionType.Convert:
                if (sqlUnaryExpression.Operand.Type == typeof(char)
                    && sqlUnaryExpression.Type.IsInteger())
                {
                    Sql.Append("unicode(");
                    Visit(sqlUnaryExpression.Operand);
                    Sql.Append(")");

                    return sqlUnaryExpression;
                }

                if (sqlUnaryExpression.Operand.Type.IsInteger()
                    && sqlUnaryExpression.Type == typeof(char))
                {
                    Sql.Append("char(");
                    Visit(sqlUnaryExpression.Operand);
                    Sql.Append(")");

                    return sqlUnaryExpression;
                }

                goto default;

            default:
                return base.VisitSqlUnary(sqlUnaryExpression);
        }
    }

    protected override bool TryGetOperatorInfo(SqlExpression expression, out int precedence, out bool isAssociative)
    {
        (precedence, isAssociative) = expression switch
        {
            SqlBinaryExpression sqlBinaryExpression => sqlBinaryExpression.OperatorType switch
            {
                ExpressionType.Multiply => (900, true),
                ExpressionType.Divide => (900, false),
                ExpressionType.Modulo => (900, false),
                ExpressionType.Add when sqlBinaryExpression.Type == typeof(string) => (1000, true),
                ExpressionType.Add when sqlBinaryExpression.Type != typeof(string) => (800, true),
                ExpressionType.Subtract => (800, false),
                ExpressionType.And => (600, true),
                ExpressionType.Or => (600, true),
                ExpressionType.LessThan => (500, false),
                ExpressionType.LessThanOrEqual => (500, false),
                ExpressionType.GreaterThan => (500, false),
                ExpressionType.GreaterThanOrEqual => (500, false),
                ExpressionType.Equal => (500, false),
                ExpressionType.NotEqual => (500, false),
                ExpressionType.AndAlso => (200, true),
                ExpressionType.OrElse => (100, true),

                _ => default,
            },

            SqlUnaryExpression sqlUnaryExpression => sqlUnaryExpression.OperatorType switch
            {
                ExpressionType.Convert => (1300, false),
                ExpressionType.Not when sqlUnaryExpression.Type != typeof(bool) => (1200, false),
                ExpressionType.Negate => (1200, false),
                ExpressionType.Equal => (500, false), // IS NULL
                ExpressionType.NotEqual => (500, false), // IS NOT NULL
                ExpressionType.Not when sqlUnaryExpression.Type == typeof(bool) => (300, false),

                _ => default,
            },

            CollateExpression => (1100, false),
            LikeExpression => (500, false),
            JsonScalarExpression => (1000, true),

            _ => default,
        };

        return precedence != default;
    }
}
