using System;
using System.Threading.Tasks;
using Rebus.Pipeline;
using Rebus.SqlServer.Transport;
using Rebus.Transport;

namespace Rebus.SqlServer.Sagas;

/// <summary>
/// Sets a savepoint on the SQL Server transport's transaction before the sagas are loaded, and rolls back to it when anything
/// after that throws: loading the sagas, the handlers, or saving the sagas one after the other.
/// </summary>
/// <remarks>
/// Rebus commits the transaction a failed attempt ran in when it dead-letters the message or sends it to 2nd level retry. Without
/// this, the work the attempt did before it failed would be committed with it: a saga update that lost a conflict halfway (its
/// index rows deleted), an earlier saga's update when a later one fails, and handler writes made through the transport's
/// connection when a saga save fails after the handlers are done.
/// </remarks>
class SagaAttemptSavepointStep : IIncomingStep
{
    const string SavepointName = "RebusSagaAttempt";

    public async Task Process(IncomingStepContext context, Func<Task> next)
    {
        var transaction = await GetTransportTransaction(context.Load<ITransactionContext>()).ConfigureAwait(false);

        // synchronous: SqlTransaction has no SaveAsync on netstandard2.0
        transaction.Save(SavepointName);

        try
        {
            await next().ConfigureAwait(false);
        }
        catch
        {
            try
            {
                transaction.Rollback(SavepointName);
            }
            catch (Exception)
            {
                // must not replace the exception being rethrown. If rolling back to the savepoint fails, the transaction is
                // already doomed (e.g. a deadlock victim), and the transport won't commit it.
            }

            throw;
        }
    }

    static async Task<Microsoft.Data.SqlClient.SqlTransaction> GetTransportTransaction(ITransactionContext transactionContext)
    {
        if (!transactionContext.Items.TryGetValue(SqlServerTransport.CurrentConnectionKey, out var item)
            || item is not Task<IDbConnection> transportConnectionTask)
        {
            throw new InvalidOperationException($"Expected the SQL Server transport to have stored its connection under '{SqlServerTransport.CurrentConnectionKey}' in the current transaction context");
        }

        var transportConnection = await transportConnectionTask.ConfigureAwait(false);

        return transportConnection.Transaction
               ?? throw new InvalidOperationException("Expected the SQL Server transport's connection to have a SqlTransaction to set a savepoint in, but it has none (is it enlisted in an ambient transaction?)");
    }
}
