namespace Apache.Ignite.EntityFrameworkCore.FunctionalTests.Query;

using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.TestUtilities;

public class NorthwindWhereQueryIgniteTest : NorthwindWhereQueryRelationalTestBase<
    NorthwindQuerySqlServerFixture<NoopModelCustomizer>>
{

}
