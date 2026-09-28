using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using NUnit.Framework;
using Rebus.Config;
using Rebus.Logging;
using Rebus.Messages;
using Rebus.SqlServer.Timeouts;
using Rebus.SqlServer.Transport;
using Rebus.Tests.Contracts;
using Rebus.Threading.TaskParallelLibrary;
using Rebus.Time;
using Rebus.Transport;

namespace Rebus.SqlServer.Tests;

[TestFixture]
[Description("ConnectionLocker must give each connection its own lock. Connections are made to collide here by giving them the same hash code.")]
public class TestConnectionLocker : FixtureBase
{
    static readonly TimeSpan MaxWait = TimeSpan.FromSeconds(3);

    [Test]
    public async Task LocksOnDifferentConnectionsDoNotWaitForEachOther()
    {
        var first = new CollidingConnection();
        var second = new CollidingConnection();

        using (await ConnectionLocker.Instance.GetLockAsync(first))
        {
            var secondLock = ConnectionLocker.Instance.GetLockAsync(second).AsTask();

            try
            {
                Assert.That(await Task.WhenAny(secondLock, Task.Delay(MaxWait)), Is.SameAs(secondLock),
                    "The lock on the second connection waited for the lock on the first connection");
            }
            finally
            {
                ReleaseWhenAcquired(secondLock);
            }
        }
    }

    [Test]
    public void SynchronousLocksOnDifferentConnectionsDoNotWaitForEachOther()
    {
        var first = new CollidingConnection();
        var second = new CollidingConnection();

        using (ConnectionLocker.Instance.GetLock(first))
        {
            var secondLock = Task.Run(() => ConnectionLocker.Instance.GetLock(second));

            try
            {
                Assert.That(secondLock.Wait(MaxWait), Is.True, "The lock on the second connection waited for the lock on the first connection");
            }
            finally
            {
                ReleaseWhenAcquired(secondLock);
            }
        }
    }

    [Test]
    public async Task LockOnTheSameConnectionIsExclusive()
    {
        var connection = new CollidingConnection();

        var firstLock = await ConnectionLocker.Instance.GetLockAsync(connection);
        var secondLock = ConnectionLocker.Instance.GetLockAsync(connection).AsTask();

        try
        {
            await Task.Delay(TimeSpan.FromMilliseconds(300));

            Assert.That(secondLock.IsCompleted, Is.False, "Got a second lock on a connection that was already locked");

            firstLock.Dispose();

            Assert.That(await Task.WhenAny(secondLock, Task.Delay(MaxWait)), Is.SameAs(secondLock), "Did not get the lock after it was released");
        }
        finally
        {
            ReleaseWhenAcquired(secondLock);
        }
    }

    [Test, Category(Categories.SqlServer)]
    [Description("SqlServerTimeoutManager holds the lock on its connection while Rebus sends the due messages, each on another connection - that must not wait for the timeout manager's lock")]
    public async Task SendingDueMessagesDoesNotWaitForTheTimeoutManagersLock()
    {
        var timeoutsTableName = TestConfig.GetName("locker-timeouts");
        var queueName = TestConfig.GetName("locker-queue");

        SqlTestHelper.DropTable(timeoutsTableName);
        SqlTestHelper.DropTable(queueName);
        Using(new DisposableCallback(() => SqlTestHelper.DropTable(timeoutsTableName)));
        Using(new DisposableCallback(() => SqlTestHelper.DropTable(queueName)));

        var loggerFactory = new ConsoleLoggerFactory(colored: false) { MinLevel = LogLevel.Warn };
        var rebusTime = new DefaultRebusTime();
        var connectionProvider = new CollidingConnectionProvider(new DbConnectionProvider(SqlTestHelper.ConnectionString, loggerFactory));

        var timeoutManager = new SqlServerTimeoutManager(connectionProvider, connectionProvider, timeoutsTableName, loggerFactory, rebusTime);
        timeoutManager.EnsureTableIsCreated();

        var transport = Using(new SqlServerTransport(connectionProvider, queueName, loggerFactory, new TplAsyncTaskFactory(loggerFactory), rebusTime, new SqlServerTransportOptions(connectionProvider)));
        transport.EnsureTableIsCreated();

        var headers = new Dictionary<string, string>
        {
            [Headers.MessageId] = Guid.NewGuid().ToString(),
            [Headers.DeferredRecipient] = queueName
        };

        await timeoutManager.Defer(DateTimeOffset.Now.AddMinutes(-1), headers, [1, 2, 3]);

        var stopwatch = Stopwatch.StartNew();

        // same as Rebus' HandleDeferredMessagesStep
        using (var result = await timeoutManager.GetDueMessages())
        {
            foreach (var dueMessage in result)
            {
                var transportMessage = dueMessage.ToTransportMessage();

                using (var scope = new RebusTransactionScope())
                {
                    await transport.Send(transportMessage.Headers[Headers.DeferredRecipient], transportMessage, scope.TransactionContext);
                    await scope.CompleteAsync();
                }

                await dueMessage.MarkAsCompleted();
            }

            await result.Complete();
        }

        Assert.That(stopwatch.Elapsed, Is.LessThan(MaxWait), "Sending the due message waited for the timeout manager's lock");
        Assert.That(SqlTestHelper.Query<CountRow>($"SELECT COUNT(*) AS [Count] FROM [dbo].[{queueName}]").Single().Count, Is.EqualTo(1));
    }

    // makes sure a lock acquired after a failed assertion does not stay taken and affect other tests
    static void ReleaseWhenAcquired(Task<IDisposable> lockTask) =>
        lockTask.ContinueWith(t =>
        {
            if (t.Status == TaskStatus.RanToCompletion) t.Result.Dispose();
        });

    class CountRow
    {
        public int Count { get; set; }
    }

    class CollidingConnectionProvider(IDbConnectionProvider inner) : IDbConnectionProvider
    {
        public async Task<IDbConnection> GetConnection() => new CollidingConnection(await inner.GetConnection());
    }

    class CollidingConnection(IDbConnection inner = null) : IDbConnection
    {
        public override int GetHashCode() => 42;

        public override bool Equals(object obj) => ReferenceEquals(this, obj);

        public SqlConnection Connection => inner.Connection;

        public SqlTransaction Transaction => inner.Transaction;

        public SqlCommand CreateCommand() => inner.CreateCommand();

        public IEnumerable<TableName> GetTableNames() => inner.GetTableNames();

        public Task Complete() => inner.Complete();

        public IEnumerable<DbColumn> GetColumns(string schema, string dataTableName) => inner.GetColumns(schema, dataTableName);

        public void Dispose() => inner?.Dispose();
    }
}
