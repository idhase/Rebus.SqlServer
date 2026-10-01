using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using NUnit.Framework;
using Rebus.Activation;
using Rebus.Bus;
using Rebus.Config;
using Rebus.Handlers;
using Rebus.Pipeline;
using Rebus.Pipeline.Receive;
using Rebus.Retry.Simple;
using Rebus.Sagas;
using Rebus.SqlServer.Transport;
using Rebus.Tests.Contracts;
using Rebus.Transport;
using Rebus.Transport.InMem;
// ReSharper disable AccessToDisposedClosure
#pragma warning disable CS1998

namespace Rebus.SqlServer.Tests.Sagas;

[TestFixture(true, TransportConnection.ConnectionString)]
[TestFixture(false, TransportConnection.ConnectionString)]
[TestFixture(true, TransportConnection.Factory)]
[TestFixture(false, TransportConnection.Factory)]
[Category(Categories.SqlServer)]
[Description("Saga data must be committed in the transport's SQL transaction, so a failing commit rolls back the saga update together with the receive and the sends")]
public class TestSagaStorageUsingTransportConnection : FixtureBase
{
    // Factory mirrors Idha.Rebus.NPoco's RebusDatabaseContext: a plain SqlConnection (no MARS) with a transaction begun by the factory
    public enum TransportConnection { ConnectionString, Factory }

    readonly bool _readCommittedSnapshot;
    readonly TransportConnection _transportConnection;
    readonly string _databaseName;
    string _connectionString;
    string _queueName;

    public TestSagaStorageUsingTransportConnection(bool readCommittedSnapshot, TransportConnection transportConnection)
    {
        _readCommittedSnapshot = readCommittedSnapshot;
        _transportConnection = transportConnection;
        _databaseName = $"{SqlTestHelper.DatabaseName}_sagatx_{(readCommittedSnapshot ? "rcsi" : "locking")}_{transportConnection.ToString().ToLowerInvariant()}";
    }

    [OneTimeSetUp]
    public void CreateDatabase()
    {
        _connectionString = new SqlConnectionStringBuilder(SqlTestHelper.ConnectionString) { InitialCatalog = _databaseName }.ConnectionString;

        var onOrOff = _readCommittedSnapshot ? "ON" : "OFF";

        // start from scratch, also when an aborted run left the database behind
        DropDatabase();

        ExecuteOnMaster($"CREATE DATABASE [{_databaseName}]");
        ExecuteOnMaster($"ALTER DATABASE [{_databaseName}] SET READ_COMMITTED_SNAPSHOT {onOrOff} WITH ROLLBACK IMMEDIATE");
    }

    [OneTimeTearDown]
    public void DropDatabase()
    {
        SqlConnection.ClearAllPools();

        ExecuteOnMaster($"IF DB_ID('{_databaseName}') IS NOT NULL BEGIN ALTER DATABASE [{_databaseName}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE [{_databaseName}] END");
    }

    protected override void SetUp()
    {
        base.SetUp();

        _queueName = $"sagatx-{Guid.NewGuid():N}";
    }

    [TestCase(true)]
    [TestCase(false)]
    public async Task SagaUpdateIsRolledBackWhenTransportCommitFails(bool sendOnlyOnce)
    {
        var scenario = new Scenario { FailCommitOnAttempt = 1 };

        var probes = StartBus(scenario, workers: 1);

        await Bus.SendLocal(new Count(Guid.NewGuid(), sendOnlyOnce));

        await WaitForProbes(probes, 1);

        Assert.That(probes, Is.EqualTo(new[] { "1" }), "The first attempt's saga update must not survive its failed commit");
        Assert.That(scenario.Attempts, Is.EqualTo(2));
    }

    [Test]
    public async Task SagaUpdateIsRolledBackWhenHandlerThrows()
    {
        var scenario = new Scenario { ThrowOnAttempt = 1 };

        var probes = StartBus(scenario, workers: 1);

        await Bus.SendLocal(new Count(Guid.NewGuid(), SendOnlyOnce: false));

        await WaitForProbes(probes, 1);

        Assert.That(probes, Is.EqualTo(new[] { "1" }));
        Assert.That(scenario.Attempts, Is.EqualTo(2));
    }

    [Test]
    public async Task SagaDataIsPersistedAcrossMessages()
    {
        var probes = StartBus(new Scenario(), workers: 1);
        var sagaId = Guid.NewGuid();

        await Bus.SendLocal(new Count(sagaId, SendOnlyOnce: false));
        await WaitForProbes(probes, 1);

        await Bus.SendLocal(new Count(sagaId, SendOnlyOnce: false));
        await WaitForProbes(probes, 2);

        Assert.That(probes, Is.EqualTo(new[] { "1", "2" }));
    }

    [Test]
    public async Task ConcurrentMessagesToTheSameSagaAreEachCountedOnce()
    {
        const int messageCount = 30;

        var probes = StartBus(new Scenario(), workers: 5);
        var sagaId = Guid.NewGuid();

        await Task.WhenAll(Enumerable.Range(0, messageCount).Select(_ => Bus.SendLocal(new Count(sagaId, SendOnlyOnce: false))));

        await WaitForProbes(probes, messageCount);

        var expected = Enumerable.Range(1, messageCount).Select(n => n.ToString());

        Assert.That(probes, Is.EquivalentTo(expected), "Every message must increment the saga exactly once, and every count must be sent exactly once");
    }

    [TestCase(true)]
    [TestCase(false)]
    [Description("Like Idha.Rebus.NPoco: a step before Dispatch sets a savepoint on the transport's transaction, and the handler writes through it")]
    public async Task HandlerWritesAndSagaUpdateRollBackTogether(bool failCommit)
    {
        var scenario = failCommit
            ? new Scenario { FailCommitOnAttempt = 1, WorkTable = CreateWorkTable() }
            : new Scenario { ThrowOnAttempt = 1, WorkTable = CreateWorkTable() };

        var probes = StartBus(scenario, workers: 1, withSavepointStep: true);

        await Bus.SendLocal(new Count(Guid.NewGuid(), SendOnlyOnce: false));

        await WaitForProbes(probes, 1);

        Assert.That(probes, Is.EqualTo(new[] { "1" }));
        Assert.That(CountRows(scenario.WorkTable), Is.EqualTo(1), "Only the successful attempt's handler write may survive");
    }

    [Test]
    [Description("On the 2nd level retry path the transaction is committed after the handler failed, with the IFailed<T> handler's work in it")]
    public async Task SagaUpdateFromFailedHandlerIsRolledBackWhenTransportCommitFails()
    {
        var scenario = new Scenario { FailCommitOnAttempt = 1 };

        var probes = StartBus(scenario, workers: 1, maxDeliveryAttempts: 1, secondLevelRetries: true);

        await Bus.SendLocal(new Explode(Guid.NewGuid()));

        await WaitForProbes(probes, 1);

        Assert.That(probes, Is.EqualTo(new[] { "failed:1" }), "The first IFailed<T> attempt's saga update must not survive its failed commit");
    }

    IBus Bus { get; set; }

    ConcurrentQueue<string> StartBus(Scenario scenario, int workers, bool withSavepointStep = false, int maxDeliveryAttempts = 100, bool secondLevelRetries = false)
    {
        var probes = new ConcurrentQueue<string>();
        var activator = Using(new BuiltinHandlerActivator());

        activator.Register((bus, _) => new CountingSaga(bus, scenario));
        activator.Register((bus, _) => new FailedHandlerSaga(bus, scenario));
        activator.Handle<Probe>(async probe => probes.Enqueue(probe.Value));

        Bus = Configure.With(activator)
            .Transport(t => t.UseSqlServer(GetTransportOptions(), _queueName))
            .Sagas(s => s.StoreInSqlServerUsingTransportConnection($"{_queueName}-data", $"{_queueName}-index"))
            .Options(o =>
            {
                o.SetNumberOfWorkers(workers);
                o.SetMaxParallelism(workers);
                o.RetryStrategy($"{_queueName}-error", maxDeliveryAttempts: maxDeliveryAttempts, secondLevelRetriesEnabled: secondLevelRetries);

                if (withSavepointStep)
                {
                    o.Decorate<IPipeline>(c => new PipelineStepInjector(c.Get<IPipeline>())
                        .OnReceive(new SavepointStep(), PipelineRelativePosition.Before, typeof(DispatchIncomingMessageStep)));
                }
            })
            .Start();

        return probes;
    }

    string CreateWorkTable()
    {
        var tableName = $"{_queueName}-work";

        ExecuteOnTestDatabase($"CREATE TABLE [{tableName}] ([SagaId] UNIQUEIDENTIFIER NOT NULL, [Attempt] INT NOT NULL)");

        return tableName;
    }

    int CountRows(string tableName)
    {
        using var connection = new SqlConnection(_connectionString);
        connection.Open();

        using var command = connection.CreateCommand();
        command.CommandText = $"SELECT COUNT(*) FROM [{tableName}]";
        return (int)command.ExecuteScalar();
    }

    void ExecuteOnTestDatabase(string sql)
    {
        using var connection = new SqlConnection(_connectionString);
        connection.Open();

        using var command = connection.CreateCommand();
        command.CommandText = sql;
        command.ExecuteNonQuery();
    }

    // mirrors Idha.Rebus.NPoco's NPocoDatabaseStep: savepoint on the transport's transaction, rolled back to if the handler throws
    class SavepointStep : IIncomingStep
    {
        const string SavepointName = "TestSavePoint";

        public async Task Process(IncomingStepContext context, Func<Task> next)
        {
            var transactionContext = context.Load<ITransactionContext>();
            var connection = await transactionContext.GetOrThrow<Task<IDbConnection>>(SqlServerTransport.CurrentConnectionKey);

            await connection.Transaction.SaveAsync(SavepointName);

            context.Save(connection);

            try
            {
                await next();
            }
            catch
            {
                await connection.Transaction.RollbackAsync(SavepointName);
                throw;
            }
        }
    }

    SqlServerTransportOptions GetTransportOptions() => _transportConnection switch
    {
        TransportConnection.ConnectionString => new SqlServerTransportOptions(_connectionString),
        TransportConnection.Factory => new SqlServerTransportOptions(async () =>
        {
            var connection = new SqlConnection(_connectionString);
            await connection.OpenAsync();
            return new DbConnectionWrapper(connection, connection.BeginTransaction(), managedExternally: false);
        }),
        _ => throw new ArgumentOutOfRangeException(nameof(_transportConnection), _transportConnection, null)
    };

    // waits for the expected number of probes, then a little longer to give surplus probes a chance to show up
    static async Task WaitForProbes(ConcurrentQueue<string> probes, int expectedCount)
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));

        while (probes.Count < expectedCount)
        {
            await Task.Delay(50, timeout.Token);
        }

        await Task.Delay(TimeSpan.FromSeconds(1));
    }

    void ExecuteOnMaster(string sql)
    {
        var masterConnectionString = new SqlConnectionStringBuilder(SqlTestHelper.ConnectionString) { InitialCatalog = "master" }.ConnectionString;

        using var connection = new SqlConnection(masterConnectionString);
        connection.Open();

        using var command = connection.CreateCommand();
        command.CommandText = sql;
        command.ExecuteNonQuery();
    }

    class Scenario
    {
        int _attempts;

        public int FailCommitOnAttempt { get; init; }
        public int ThrowOnAttempt { get; init; }
        public string WorkTable { get; init; }
        public int Attempts => _attempts;
        public int NextAttempt() => Interlocked.Increment(ref _attempts);
    }

    class CountingSaga : Saga<CountingSagaData>, IAmInitiatedBy<Count>
    {
        readonly IBus _bus;
        readonly Scenario _scenario;

        public CountingSaga(IBus bus, Scenario scenario)
        {
            _bus = bus;
            _scenario = scenario;
        }

        protected override void CorrelateMessages(ICorrelationConfig<CountingSagaData> config)
        {
            config.Correlate<Count>(m => m.SagaId, d => d.CorrelationId);
        }

        public async Task Handle(Count message)
        {
            var attempt = _scenario.NextAttempt();

            Data.CorrelationId = message.SagaId;
            Data.Count++;

            if (_scenario.WorkTable != null)
            {
                var connection = MessageContext.Current.IncomingStepContext.Load<IDbConnection>();

                using var command = connection.CreateCommand();
                command.CommandText = $"INSERT INTO [{_scenario.WorkTable}] ([SagaId], [Attempt]) VALUES (@sagaId, @attempt)";
                command.Parameters.AddWithValue("@sagaId", message.SagaId);
                command.Parameters.AddWithValue("@attempt", attempt);
                await command.ExecuteNonQueryAsync();
            }

            // a saga guarding its sends with its own state loses them for good if a stale update survives a failed commit
            if (!message.SendOnlyOnce || !Data.Sent)
            {
                Data.Sent = true;
                await _bus.SendLocal(new Probe(Data.Count.ToString()));
            }

            if (attempt == _scenario.FailCommitOnAttempt)
            {
                MessageContext.Current.TransactionContext.OnCommit(async _ => throw new InvalidOperationException("commit fails after the saga data was saved"));
            }

            if (attempt == _scenario.ThrowOnAttempt)
            {
                throw new InvalidOperationException("handler fails");
            }
        }
    }

    class CountingSagaData : SagaData
    {
        public Guid CorrelationId { get; set; }
        public int Count { get; set; }
        public bool Sent { get; set; }
    }

    class FailedHandlerSaga : Saga<FailedHandlerSagaData>, IAmInitiatedBy<Explode>, IAmInitiatedBy<IFailed<Explode>>
    {
        readonly IBus _bus;
        readonly Scenario _scenario;

        public FailedHandlerSaga(IBus bus, Scenario scenario)
        {
            _bus = bus;
            _scenario = scenario;
        }

        protected override void CorrelateMessages(ICorrelationConfig<FailedHandlerSagaData> config)
        {
            config.Correlate<Explode>(m => m.SagaId, d => d.CorrelationId);
            config.Correlate<IFailed<Explode>>(m => m.Message.SagaId, d => d.CorrelationId);
        }

        public Task Handle(Explode message) => throw new InvalidOperationException("always fails");

        public async Task Handle(IFailed<Explode> failed)
        {
            var attempt = _scenario.NextAttempt();

            Data.CorrelationId = failed.Message.SagaId;
            Data.Failures++;

            await _bus.SendLocal(new Probe($"failed:{Data.Failures}"));

            if (attempt == _scenario.FailCommitOnAttempt)
            {
                MessageContext.Current.TransactionContext.OnCommit(async _ => throw new InvalidOperationException("commit fails after the saga data was saved"));
            }
        }
    }

    class FailedHandlerSagaData : SagaData
    {
        public Guid CorrelationId { get; set; }
        public int Failures { get; set; }
    }

    record Count(Guid SagaId, bool SendOnlyOnce);

    record Explode(Guid SagaId);

    record Probe(string Value);
}

[TestFixture, Category(Categories.SqlServer)]
public class TestSagaStorageUsingTransportConnectionConfiguration : FixtureBase
{
    [Test]
    public void ThrowsAtStartupWithoutTheSqlServerTransport()
    {
        var exception = StartAndCatch(t => t.UseInMemoryTransport(new InMemNetwork(), "sagatx-inmem"));

        Assert.That(exception.ToString(), Does.Contain("requires the SQL Server transport"));
    }

    [Test]
    public void ThrowsAtStartupWithTheLeaseTransport()
    {
        var queueName = $"sagatx-lease-{Guid.NewGuid():N}";

        Using(new DisposableCallback(() => SqlTestHelper.DropTable(queueName)));

        var exception = StartAndCatch(t => t.UseSqlServerInLeaseMode(new SqlServerLeaseTransportOptions(SqlTestHelper.ConnectionString), queueName));

        Assert.That(exception.ToString(), Does.Contain("does not work with the lease-based SQL Server transport"));
    }

    [Test]
    public void ThrowsAtStartupWithAOneWayClient()
    {
        var exception = StartAndCatch(t => t.UseSqlServerAsOneWayClient(new SqlServerTransportOptions(SqlTestHelper.ConnectionString)));

        Assert.That(exception.ToString(), Does.Contain("one-way client"));
    }

    Exception StartAndCatch(Action<StandardConfigurer<ITransport>> configureTransport)
    {
        var activator = Using(new BuiltinHandlerActivator());
        var sagaTable = $"sagatx-cfg-{Guid.NewGuid():N}";

        Using(new DisposableCallback(() => SqlTestHelper.DropTable($"{sagaTable}-data")));
        Using(new DisposableCallback(() => SqlTestHelper.DropTable($"{sagaTable}-index")));

        try
        {
            Using(Configure.With(activator)
                .Transport(configureTransport)
                .Sagas(s => s.StoreInSqlServerUsingTransportConnection($"{sagaTable}-data", $"{sagaTable}-index"))
                .Start());
        }
        catch (Exception exception)
        {
            return exception;
        }

        Assert.Fail("Expected the bus to fail at startup");
        return null;
    }
}

[TestFixture]
public class TestSqlServerTransportConnectionProvider
{
    [Test]
    public async Task UsesTheFallbackOutsideOfMessageHandling()
    {
        var fallbackConnection = new DbConnectionWrapper(new SqlConnection(), null, managedExternally: true);
        var provider = new SqlServerTransportConnectionProvider(new FixedConnectionProvider(fallbackConnection));

        var connection = await provider.GetConnection();

        Assert.That(connection, Is.SameAs(fallbackConnection));
    }

    [Test]
    public async Task SharesTheTransportConnectionInsideOfMessageHandling()
    {
        var transportSqlConnection = new SqlConnection();
        var provider = new SqlServerTransportConnectionProvider(new FixedConnectionProvider(null));

        using var scope = new RebusTransactionScope();
        scope.TransactionContext.Items[SqlServerTransport.CurrentConnectionKey] =
            Task.FromResult<IDbConnection>(new DbConnectionWrapper(transportSqlConnection, null, managedExternally: false));

        var connection = await provider.GetConnection();

        Assert.That(connection.Connection, Is.SameAs(transportSqlConnection));
    }

    [Test]
    public void ThrowsInsideOfMessageHandlingWhenTheTransportHasNoConnection()
    {
        var provider = new SqlServerTransportConnectionProvider(new FixedConnectionProvider(null));

        using var scope = new RebusTransactionScope();

        var exception = Assert.ThrowsAsync<InvalidOperationException>(() => provider.GetConnection());

        Assert.That(exception.Message, Does.Contain(SqlServerTransport.CurrentConnectionKey));
    }

    class FixedConnectionProvider(IDbConnection connection) : IDbConnectionProvider
    {
        public Task<IDbConnection> GetConnection() => Task.FromResult(connection);
    }
}
