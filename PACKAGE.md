# Idha.Rebus.SqlServer

Idha's fork of [Rebus.SqlServer](https://github.com/rebus-org/Rebus.SqlServer): SQL Server transport, saga storage, subscriptions, timeouts, outbox and data bus for [Rebus](https://github.com/rebus-org/Rebus).

It follows upstream closely, and configuration and usage are the same. See the [upstream README](https://github.com/rebus-org/Rebus.SqlServer#readme) for how to use it.

## How it differs from upstream

- `IDbConnection` exposes the underlying `SqlConnection` and `SqlTransaction` as `Connection` and `Transaction`, so other code can share the transport's connection and transaction. Idha.Rebus.NPoco uses this.
- Table names can include a database, as in `catalog.schema.table`. This is meant for sending to queues in other databases on the same server. Don't use it for tables that Rebus creates (input queues, sagas, subscriptions, timeouts, outbox), because the checks for whether those exist only look in the current database.
- Fixes and improvements that aren't in upstream yet, see 1.0.9 below.

## What's new in 1.0.9

### Fixes

- **Messages from a failed handler attempt are no longer sent on 2nd level retries.** When a message went on to the 2nd level retry path, the messages sent by the attempt that failed were delivered together with the ones from the `IFailed<T>` handler. A saga could, for example, get its timeout scheduled twice. Fixed for both the normal and the lease-based transport.
- **`SqlServerTimeoutManager` no longer stalls for 30 seconds.** Sending due messages could end up waiting for the timeout manager's own connection lock until a 30 second timeout cancelled it. This affects services that use `.Timeouts(t => t.StoreInSqlServer(...))`.
- **Sends on different connections no longer wait for each other.** A lock that made every insert in a process wait for the others is removed.

### Improvements

- The messages a handler sends are inserted with one multi-row INSERT per destination table, in chunks of up to 32, instead of one INSERT per message.
- New queue tables get a receive index that matches the receive query again: `([priority] DESC, [visible], [id], [expiration])`.

### Behavior changes

- Messages are now inserted when the transaction commits, still in the same SQL transaction as the receive. An error while inserting, such as a missing table, is thrown at commit instead of from `Send`. Inside a handler that's a normal retry. With a `RebusTransactionScope` it's thrown from `CompleteAsync()`.
- From upstream: the protected `SqlServerTransport.InnerSend` is now `InnerSendAsync`, and `SqlServerTimeoutManager`'s constructor takes two connection providers. `StoreInSqlServer` is already updated, so only code that creates the timeout manager with `new` needs changing.
- `SqlServerLeaseTransport.OutboundMessageBufferKey` is obsolete and no longer used.

### When upgrading

- Requires Rebus 8.9.0 and Microsoft.Data.SqlClient 6.1.2 or later.
- Existing queue tables keep their old receive index. To switch them to the new one, run [scripts/recreate-receive-index.sql](https://github.com/idhase/Rebus.SqlServer/blob/master/scripts/recreate-receive-index.sql) in each database, preferably when it's quiet. It prints the statements first, so you can review them before running them.

The full history is in [CHANGELOG.md](https://github.com/idhase/Rebus.SqlServer/blob/master/CHANGELOG.md).
