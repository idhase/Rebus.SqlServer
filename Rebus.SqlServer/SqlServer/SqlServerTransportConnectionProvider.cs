using System;
using System.Threading.Tasks;
using Rebus.SqlServer.Transport;
using Rebus.Transport;

namespace Rebus.SqlServer;

/// <summary>
/// Implementation of <see cref="IDbConnectionProvider"/> that hands out the <see cref="SqlServerTransport"/>'s connection and
/// transaction when called while a message is being handled, so work done through it commits and rolls back together with the
/// receive and the outgoing messages. Outside of message handling (e.g. when creating tables at startup) it uses the given
/// provider, which must be the transport's own.
/// </summary>
class SqlServerTransportConnectionProvider : IDbConnectionProvider
{
    readonly IDbConnectionProvider _transportConnectionProvider;

    public SqlServerTransportConnectionProvider(IDbConnectionProvider transportConnectionProvider)
    {
        _transportConnectionProvider = transportConnectionProvider ?? throw new ArgumentNullException(nameof(transportConnectionProvider));
    }

    public async Task<IDbConnection> GetConnection()
    {
        var transactionContext = AmbientTransactionContext.Current;

        if (transactionContext == null)
        {
            return await _transportConnectionProvider.GetConnection().ConfigureAwait(false);
        }

        // falling back to a connection of our own here would silently make the saga update non-atomic again
        if (!transactionContext.Items.TryGetValue(SqlServerTransport.CurrentConnectionKey, out var item)
            || item is not Task<IDbConnection> transportConnectionTask)
        {
            throw new InvalidOperationException($"Expected the SQL Server transport to have stored its connection under '{SqlServerTransport.CurrentConnectionKey}' in the current transaction context");
        }

        var transportConnection = await transportConnectionTask.ConfigureAwait(false);

        // No ConnectionLocker here, unlike the transport's own commands: the receive is done before the pipeline runs, the
        // pipeline runs its steps one at a time, and outgoing messages are inserted at commit, after the saga has been saved
        return SavepointDbConnection.Create(transportConnection.Connection, transportConnection.Transaction);
    }
}
