
namespace Apache.Ignite.EntityFrameworkCore.Infrastructure;

using Internal;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;

public class IgniteDbContextOptionsBuilder : RelationalDbContextOptionsBuilder<IgniteDbContextOptionsBuilder, IgniteOptionsExtension>
{
    /// <summary>
    ///     Initializes a new instance of the <see cref="IgniteDbContextOptionsBuilder" /> class.
    /// </summary>
    /// <param name="optionsBuilder">The options builder.</param>
    public IgniteDbContextOptionsBuilder(DbContextOptionsBuilder optionsBuilder)
        : base(optionsBuilder)
    {
    }
}
