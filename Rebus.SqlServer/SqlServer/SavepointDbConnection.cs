using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;

namespace Rebus.SqlServer;

/// <summary>
/// Wraps a connection and transaction owned by someone else, and makes the work done through it all-or-nothing within that
/// transaction: a savepoint is set when it's created, and <see cref="Dispose"/> rolls back to it unless <see cref="Complete"/> was
/// called. The owner's transaction is never committed, rolled back as a whole or closed.
/// </summary>
/// <remarks>
/// Saga storage needs this when it shares the transport's transaction. An update deletes the saga's index rows before its revision
/// check, so one that loses a conflict has done half its work when it throws, and Rebus commits the transaction it ran in when the
/// message is dead-lettered or goes to 2nd level retry.
/// </remarks>
class SavepointDbConnection : IDbConnection
{
    const string SavepointName = "RebusSqlServerSavepoint";

    readonly DbConnectionWrapper _connection;
    bool _completed;
    bool _disposed;

    SavepointDbConnection(SqlConnection connection, SqlTransaction transaction)
    {
        _connection = new DbConnectionWrapper(connection, transaction, managedExternally: true);
    }

    public static IDbConnection Create(SqlConnection connection, SqlTransaction transaction)
    {
        if (transaction == null)
        {
            throw new InvalidOperationException("Expected the SQL Server transport's connection to have a SqlTransaction to set a savepoint in, but it has none (is it enlisted in an ambient transaction?)");
        }

        // synchronous: SqlTransaction has no SaveAsync on netstandard2.0
        transaction.Save(SavepointName);

        return new SavepointDbConnection(connection, transaction);
    }

    public SqlConnection Connection => _connection.Connection;

    public SqlTransaction Transaction => _connection.Transaction;

    public SqlCommand CreateCommand() => _connection.CreateCommand();

    public IEnumerable<TableName> GetTableNames() => _connection.GetTableNames();

    public IEnumerable<DbColumn> GetColumns(string schema, string dataTableName) => _connection.GetColumns(schema, dataTableName);

    public Task Complete()
    {
        _completed = true;
        return Task.CompletedTask;
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;

        if (_completed) return;

        try
        {
            Transaction.Rollback(SavepointName);
        }
        catch (Exception)
        {
            // Dispose runs while the exception that skipped Complete is propagating, and must not replace it. If rolling back to
            // the savepoint fails, the transaction is already doomed (e.g. a deadlock victim), and the transport won't commit it.
        }
    }
}
