using System;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace Rebus.SqlServer;

/// <summary>
/// Helper that helps with getting exclusive access to a specific DB connection.
/// </summary>
class ConnectionLocker
{
    public static readonly ConnectionLocker Instance = new();

    // one semaphore per connection instance (compared by reference), which goes away together with the connection
    readonly ConditionalWeakTable<IDbConnection, SemaphoreSlim> _semaphores = new();

    public async ValueTask<IDisposable> GetLockAsync(IDbConnection connection)
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));

        var semaphore = GetSemaphore(connection);
        await semaphore.WaitAsync(timeout.Token);

        return new SemaphoreReleaser(semaphore);
    }

    public IDisposable GetLock(IDbConnection connection)
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));

        var semaphore = GetSemaphore(connection);
        semaphore.Wait(timeout.Token);

        return new SemaphoreReleaser(semaphore);
    }

    SemaphoreSlim GetSemaphore(IDbConnection connection) => _semaphores.GetValue(connection, _ => new SemaphoreSlim(initialCount: 1));

    readonly struct SemaphoreReleaser(SemaphoreSlim semaphore) : IDisposable
    {
        public void Dispose() => semaphore.Release();
    }
}
