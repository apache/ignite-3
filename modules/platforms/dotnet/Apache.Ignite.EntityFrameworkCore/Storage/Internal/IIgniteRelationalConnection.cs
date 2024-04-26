
namespace Apache.Ignite.EntityFrameworkCore.Storage.Internal;

using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.Extensions.DependencyInjection;

/// <remarks>
///     The service lifetime is <see cref="ServiceLifetime.Scoped" />. This means that each
///     <see cref="DbContext" /> instance will use its own instance of this service.
///     The implementation may depend on other services registered with any lifetime.
///     The implementation does not need to be thread-safe.
/// </remarks>
public interface IIgniteRelationalConnection : IRelationalConnection
{
    IIgniteRelationalConnection CreateReadOnlyConnection();
}
