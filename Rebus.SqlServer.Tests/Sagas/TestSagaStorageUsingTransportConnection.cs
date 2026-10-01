using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
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
using Rebus.SqlServer.Sagas;
using Rebus.SqlServer.Transport;
using Rebus.Messages;
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

    [Test]
    [Description("A burst to one saga with the usual 5 delivery attempts dead-letters some messages, which must leave the saga intact")]
    public async Task BurstToOneSagaKeepsOneSagaWhenMessagesAreDeadLettered()
    {
        const int burst = 50;

        var probes = StartBus(new Scenario(), workers: 5, maxDeliveryAttempts: 5);
        var sagaId = Guid.NewGuid();

        await Bus.SendLocal(new Count(sagaId, SendOnlyOnce: false));
        await WaitForProbes(probes, 1);

        await Task.WhenAll(Enumerable.Range(1, burst - 1).Select(_ => Bus.SendLocal(new Count(sagaId, SendOnlyOnce: false))));

        using (var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(60)))
        {
            while (probes.Count + CountRows($"{_queueName}-error") < burst)
            {
                await Task.Delay(100, timeout.Token);
            }
        }

        var expectedProbes = probes.Count + 1;
        await Bus.SendLocal(new Count(sagaId, SendOnlyOnce: false));
        await WaitForProbes(probes, expectedProbes);

        var expected = Enumerable.Range(1, probes.Count).Select(n => n.ToString());

        Assert.That(probes, Is.EquivalentTo(expected), "Every committed message must have counted on the same saga, also after others were dead-lettered");
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
    [Description("Completing a saga deletes it; if the commit then fails, the redelivered message must still find the saga")]
    public async Task SagaDeletionIsRolledBackWhenTransportCommitFails()
    {
        var scenario = new Scenario { FailCommitOnAttempt = 2 };

        var probes = StartBus(scenario, workers: 1);
        var sagaId = Guid.NewGuid();

        await Bus.SendLocal(new Count(sagaId, SendOnlyOnce: false));
        await WaitForProbes(probes, 1);

        await Bus.SendLocal(new Finish(sagaId));
        await WaitForProbes(probes, 2);

        Assert.That(probes, Is.EqualTo(new[] { "1", "finished:1" }), "The retry of the completing message must find the saga its failed attempt deleted");
        Assert.That(scenario.Attempts, Is.EqualTo(3));

        await Bus.SendLocal(new Count(sagaId, SendOnlyOnce: false));
        await WaitForProbes(probes, 3);

        Assert.That(probes.Last(), Is.EqualTo("1"), "Once completed for real, the saga is gone and the next message starts a new one");
    }

    [Test]
    [Description(@"Rebus saves a message's sagas one after the other once all handlers are done. When a later save fails and the message is
dead-lettered in the same transaction, an earlier saga's successful update must not be committed with it")]
    public async Task EarlierSagaUpdateIsRolledBackWhenALaterSagaSaveFailsAndTheMessageIsDeadLettered()
    {
        var probes = StartBus(new Scenario(), workers: 1, maxDeliveryAttempts: 1);
        var sagaId = Guid.NewGuid();

        await Bus.SendLocal(new Both(sagaId, FailSecondSave: false));
        await WaitForProbes(probes, 2);

        await Bus.SendLocal(new Both(sagaId, FailSecondSave: true));
        await WaitUntil(() => CountRows($"{_queueName}-error") == 1, "the message whose second saga save fails to be dead-lettered");

        await Bus.SendLocal(new Both(sagaId, FailSecondSave: false));
        await WaitForProbes(probes, 4);

        Assert.That(probes, Is.EquivalentTo(new[] { "first:1", "second:1", "first:2", "second:2" }), "The dead-lettered message must have left both sagas as they were");
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

    public enum OtherSaga { IsCreated, IsUpdated }

    [TestCase(OtherSaga.IsCreated, 0)]
    [TestCase(OtherSaga.IsUpdated, 0)]
    [TestCase(OtherSaga.IsCreated, 5000)]
    [TestCase(OtherSaga.IsUpdated, 5000)]
    [Description("Saga rows stay locked until the transport commits, which must not hold up messages to other sagas")]
    public async Task OtherSagasAreNotBlockedWhileASagaTransactionIsOpen(OtherSaga otherSaga, int seededSagas)
    {
        if (!_readCommittedSnapshot && seededSagas == 0)
        {
            Assert.Ignore("Without RCSI, the saga lookup on near-empty tables scans the data table and waits for other sagas' uncommitted rows. With more rows it seeks, and with RCSI it doesn't take locks");
        }

        var scenario = new Scenario();
        var probes = StartBus(scenario, workers: 2);

        SeedUnrelatedSagas(seededSagas);

        var heldSaga = Guid.NewGuid();
        var otherSagaId = Guid.NewGuid();

        try
        {
            if (otherSaga == OtherSaga.IsUpdated)
            {
                await Bus.SendLocal(new Count(heldSaga, SendOnlyOnce: false));
                await Bus.SendLocal(new Count(otherSagaId, SendOnlyOnce: false));
                await WaitForProbes(probes, 2);
            }

            var probesBefore = probes.Count;

            await Bus.SendLocal(new Count(heldSaga, SendOnlyOnce: false, HoldCommit: true));
            await scenario.CommitHeld.Task.WaitAsync(TimeSpan.FromSeconds(30));

            await Bus.SendLocal(new Count(otherSagaId, SendOnlyOnce: false));

            var otherSagaFinished = await WaitForProbeCount(probes, probesBefore + 1, TimeSpan.FromSeconds(10));

            Assert.That(otherSagaFinished, Is.True, "A message to another saga must finish while the held saga's transaction is open");
        }
        finally
        {
            scenario.ReleaseCommit.TrySetResult();
        }

        await WaitForProbes(probes, otherSaga == OtherSaga.IsUpdated ? 4 : 2);
    }

    [Test]
    [Description(@"A saga update deletes its index rows before the revision check, so one that loses a conflict has done half its work when it throws.
Rebus dead-letters a message in the same transaction and commits it, so that half must already be rolled back, or the saga can't be found any more")]
    public async Task LosingSagaUpdateLeavesNoTraceWhenItsMessageIsDeadLettered()
    {
        var scenario = new Scenario();
        var probes = StartBus(scenario, workers: 2, maxDeliveryAttempts: 1);
        var sagaId = Guid.NewGuid();

        await Bus.SendLocal(new Count(sagaId, SendOnlyOnce: false));
        await WaitForProbes(probes, 1);

        try
        {
            await Bus.SendLocal(new Count(sagaId, SendOnlyOnce: false, HoldCommit: true));
            await scenario.CommitHeld.Task.WaitAsync(TimeSpan.FromSeconds(30));

            await Bus.SendLocal(new Count(sagaId, SendOnlyOnce: false));

            // With RCSI the saga is loaded before the handler runs, without waiting for the held update, so once the handler has
            // started this message holds the old revision and its update must lose. Without RCSI the load waits for the held
            // update, so the handler only starts after the release and nothing conflicts.
            if (_readCommittedSnapshot)
            {
                await scenario.WhenAttemptStarts(3).WaitAsync(TimeSpan.FromSeconds(30));
            }
        }
        finally
        {
            scenario.ReleaseCommit.TrySetResult();
        }

        if (_readCommittedSnapshot)
        {
            await WaitUntil(() => CountRows($"{_queueName}-error") == 1, "the conflicting message to be dead-lettered");
            await WaitForProbes(probes, 2);
        }
        else
        {
            await WaitForProbes(probes, 3);
        }

        var expectedProbes = probes.Count + 1;
        await Bus.SendLocal(new Count(sagaId, SendOnlyOnce: false));
        await WaitForProbes(probes, expectedProbes);

        var expected = Enumerable.Range(1, probes.Count).Select(n => n.ToString());

        // without RCSI the second message waits instead of conflicting, and its probe can overtake the held message's
        Assert.That(probes, Is.EquivalentTo(expected), "Every committed message must have found and updated the same saga");
    }

    [Test]
    [Description("Control for the test above: shows that it can see blocking at all")]
    public async Task SameSagaIsBlockedWhileItsTransactionIsOpen()
    {
        var scenario = new Scenario();
        var probes = StartBus(scenario, workers: 2);
        var sagaId = Guid.NewGuid();

        try
        {
            await Bus.SendLocal(new Count(sagaId, SendOnlyOnce: false, HoldCommit: true));
            await scenario.CommitHeld.Task.WaitAsync(TimeSpan.FromSeconds(30));

            await Bus.SendLocal(new Count(sagaId, SendOnlyOnce: false));

            var secondFinished = await WaitForProbeCount(probes, 1, TimeSpan.FromSeconds(5));

            Assert.That(secondFinished, Is.False, "A second message to the held saga must wait for its transaction");
        }
        finally
        {
            scenario.ReleaseCommit.TrySetResult();
        }

        await WaitForProbes(probes, 2);

        Assert.That(probes, Is.EquivalentTo(new[] { "1", "2" }));
    }

    void SeedUnrelatedSagas(int count)
    {
        if (count == 0) return;

        ExecuteOnTestDatabase($@"
WITH n AS (SELECT TOP ({count}) NEWID() AS id FROM sys.all_objects a CROSS JOIN sys.all_objects b)
SELECT id INTO #ids FROM n;
INSERT INTO [{_queueName}-data] ([id], [revision], [data]) SELECT id, 0, 0x00 FROM #ids;
INSERT INTO [{_queueName}-index] ([saga_type], [key], [value], [saga_id]) SELECT 'Seeded', 'CorrelationId', CONVERT(nvarchar(200), id), id FROM #ids;");
    }

    static async Task WaitUntil(Func<bool> condition, string what)
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));

        while (!condition())
        {
            if (timeout.IsCancellationRequested) Assert.Fail($"Timed out waiting for {what}");

            await Task.Delay(50);
        }
    }

    static async Task<bool> WaitForProbeCount(ConcurrentQueue<string> probes, int expectedCount, TimeSpan within)
    {
        var deadline = DateTime.UtcNow + within;

        while (probes.Count < expectedCount)
        {
            if (DateTime.UtcNow > deadline) return false;

            await Task.Delay(50);
        }

        return true;
    }

    IBus Bus { get; set; }

    ConcurrentQueue<string> StartBus(Scenario scenario, int workers, bool withSavepointStep = false, int maxDeliveryAttempts = 100, bool secondLevelRetries = false)
    {
        var probes = new ConcurrentQueue<string>();
        var activator = Using(new BuiltinHandlerActivator());

        activator.Register((bus, _) => new CountingSaga(bus, scenario));
        activator.Register((bus, _) => new FailedHandlerSaga(bus, scenario));
        activator.Register((bus, _) => new FirstOfBothSaga(bus));
        activator.Register((bus, _) => new SecondOfBothSaga(bus));
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
        public TaskCompletionSource CommitHeld { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource ReleaseCommit { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        readonly ConcurrentDictionary<int, TaskCompletionSource> _attemptStarted = new();

        public int Attempts => _attempts;

        public Task WhenAttemptStarts(int attempt) => AttemptStarted(attempt).Task;

        public int NextAttempt()
        {
            var attempt = Interlocked.Increment(ref _attempts);
            AttemptStarted(attempt).TrySetResult();
            return attempt;
        }

        TaskCompletionSource AttemptStarted(int attempt) =>
            _attemptStarted.GetOrAdd(attempt, _ => new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously));
    }

    class CountingSaga : Saga<CountingSagaData>, IAmInitiatedBy<Count>, IHandleMessages<Finish>
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
            config.Correlate<Finish>(m => m.SagaId, d => d.CorrelationId);
        }

        public async Task Handle(Finish message)
        {
            var attempt = _scenario.NextAttempt();

            await _bus.SendLocal(new Probe($"finished:{Data.Count}"));

            MarkAsComplete();

            if (attempt == _scenario.FailCommitOnAttempt)
            {
                MessageContext.Current.TransactionContext.OnCommit(async _ => throw new InvalidOperationException("commit fails after the saga was deleted"));
            }
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

            if (message.HoldCommit)
            {
                // the transport commits its SQL transaction in OnAck, so this keeps the saga's rows locked until released
                MessageContext.Current.TransactionContext.OnCommit(async _ =>
                {
                    _scenario.CommitHeld.TrySetResult();
                    await _scenario.ReleaseCommit.Task;
                });
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

    record Count(Guid SagaId, bool SendOnlyOnce, bool HoldCommit = false);

    record Finish(Guid SagaId);

    record Both(Guid SagaId, bool FailSecondSave);

    class FirstOfBothSaga : Saga<FirstOfBothSagaData>, IAmInitiatedBy<Both>
    {
        readonly IBus _bus;

        public FirstOfBothSaga(IBus bus) => _bus = bus;

        protected override void CorrelateMessages(ICorrelationConfig<FirstOfBothSagaData> config) =>
            config.Correlate<Both>(m => m.SagaId, d => d.CorrelationId);

        public async Task Handle(Both message)
        {
            Data.CorrelationId = message.SagaId;
            Data.Count++;
            await _bus.SendLocal(new Probe($"first:{Data.Count}"));
        }
    }

    class FirstOfBothSagaData : SagaData
    {
        public Guid CorrelationId { get; set; }
        public int Count { get; set; }
    }

    class SecondOfBothSaga : Saga<SecondOfBothSagaData>, IAmInitiatedBy<Both>
    {
        readonly IBus _bus;

        public SecondOfBothSaga(IBus bus) => _bus = bus;

        protected override void CorrelateMessages(ICorrelationConfig<SecondOfBothSagaData> config) =>
            config.Correlate<Both>(m => m.SagaId, d => d.CorrelationId);

        public async Task Handle(Both message)
        {
            Data.CorrelationId = message.SagaId;
            Data.Count++;
            Data.FailSave = message.FailSecondSave;
            await _bus.SendLocal(new Probe($"second:{Data.Count}"));
        }
    }

    class SecondOfBothSagaData : SagaData
    {
        public Guid CorrelationId { get; set; }
        public int Count { get; set; }
        public bool FailSave { get; set; }

        // makes saving this saga throw after the handlers are done, when the other saga has already been saved
        public string Unsaveable => FailSave ? throw new InvalidOperationException("this saga data can't be serialized") : null;
    }

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
    public void ThrowsAtStartupWhenTheTransportEnlistsInAmbientTransactions()
    {
        var queueName = $"sagatx-enlist-{Guid.NewGuid():N}";

        Using(new DisposableCallback(() => SqlTestHelper.DropTable(queueName)));

        var exception = StartAndCatch(t => t.UseSqlServer(new SqlServerTransportOptions(SqlTestHelper.ConnectionString, enlistInAmbientTransaction: true), queueName));

        Assert.That(exception.ToString(), Does.Contain("SqlTransaction of its own"));
    }

    [Test]
    public void ThrowsAtStartupWhenTheTransportConnectionFactoryGivesNoTransaction()
    {
        var queueName = $"sagatx-notx-{Guid.NewGuid():N}";

        Using(new DisposableCallback(() => SqlTestHelper.DropTable(queueName)));

        var exception = StartAndCatch(t => t.UseSqlServer(new SqlServerTransportOptions(async () =>
        {
            var connection = new SqlConnection(SqlTestHelper.ConnectionString);
            await connection.OpenAsync();
            return new DbConnectionWrapper(connection, null, managedExternally: false);
        }), queueName));

        Assert.That(exception.ToString(), Does.Contain("SqlTransaction of its own"));
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

    [Test, Category(Categories.SqlServer)]
    public async Task SharesTheTransportConnectionAndTransactionInsideOfMessageHandling()
    {
        using var transport = OpenTransportConnection();
        using var scope = InScopeWith(transport);

        using var connection = await NewProvider().GetConnection();

        Assert.That(connection.Connection, Is.SameAs(transport.Connection));
        Assert.That(connection.Transaction, Is.SameAs(transport.Transaction));
    }

    [Test, Category(Categories.SqlServer)]
    public async Task DisposingTheSharedConnectionLeavesTheTransportsTransactionAlone()
    {
        using var transport = OpenTransportConnection();
        using var scope = InScopeWith(transport);

        using (var connection = await NewProvider().GetConnection())
        {
            Execute(connection, "INSERT INTO #work VALUES (1)");
            await connection.Complete();
        }

        Assert.That(CountRows(transport), Is.EqualTo(1), "Neither Complete nor Dispose may commit, roll back or close the transport's transaction");
        Assert.That(transport.Transaction.Connection, Is.Not.Null);
    }

    [Test, Category(Categories.SqlServer)]
    public async Task AttemptStepRollsBackToItsSavepointWhenTheRestOfTheAttemptThrows()
    {
        using var transport = OpenTransportConnection();
        using var scope = InScopeWith(transport);

        Execute(transport, "INSERT INTO #work VALUES (1)");

        var context = new IncomingStepContext(new TransportMessage(new Dictionary<string, string>(), Array.Empty<byte>()), scope.TransactionContext);

        Assert.ThrowsAsync<InvalidOperationException>(() => new SagaAttemptSavepointStep().Process(context, () =>
        {
            Execute(transport, "INSERT INTO #work VALUES (2)");
            throw new InvalidOperationException("the attempt fails");
        }));

        Assert.That(CountRows(transport), Is.EqualTo(1), "Only the attempt's work is rolled back");
        Assert.That(transport.Transaction.Connection, Is.Not.Null, "The transport's transaction must still be usable");
    }

    [Test, Category(Categories.SqlServer)]
    public async Task AttemptStepKeepsTheWorkWhenTheAttemptSucceeds()
    {
        using var transport = OpenTransportConnection();
        using var scope = InScopeWith(transport);

        var context = new IncomingStepContext(new TransportMessage(new Dictionary<string, string>(), Array.Empty<byte>()), scope.TransactionContext);

        await new SagaAttemptSavepointStep().Process(context, () =>
        {
            Execute(transport, "INSERT INTO #work VALUES (2)");
            return Task.CompletedTask;
        });

        Assert.That(CountRows(transport), Is.EqualTo(1));
        Assert.That(transport.Transaction.Connection, Is.Not.Null, "The step must not commit the transport's transaction");
    }

    [Test]
    public void AttemptStepThrowsWhenTheTransportConnectionHasNoTransaction()
    {
        using var scope = new RebusTransactionScope();
        scope.TransactionContext.Items[SqlServerTransport.CurrentConnectionKey] =
            Task.FromResult<IDbConnection>(new DbConnectionWrapper(new SqlConnection(), null, managedExternally: true));

        var context = new IncomingStepContext(new TransportMessage(new Dictionary<string, string>(), Array.Empty<byte>()), scope.TransactionContext);

        var exception = Assert.ThrowsAsync<InvalidOperationException>(() => new SagaAttemptSavepointStep().Process(context, () => Task.CompletedTask));

        Assert.That(exception.Message, Does.Contain("SqlTransaction"));
    }

    static SqlServerTransportConnectionProvider NewProvider() => new(new FixedConnectionProvider(null));

    static IDbConnection OpenTransportConnection()
    {
        var connection = new SqlConnection(SqlTestHelper.ConnectionString);
        connection.Open();

        var transport = new DbConnectionWrapper(connection, connection.BeginTransaction(), managedExternally: false);

        Execute(transport, "CREATE TABLE #work ([id] INT NOT NULL)");

        return transport;
    }

    static RebusTransactionScope InScopeWith(IDbConnection transport)
    {
        var scope = new RebusTransactionScope();
        scope.TransactionContext.Items[SqlServerTransport.CurrentConnectionKey] = Task.FromResult(transport);
        return scope;
    }

    static void Execute(IDbConnection connection, string sql)
    {
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        command.ExecuteNonQuery();
    }

    static int CountRows(IDbConnection connection)
    {
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT COUNT(*) FROM #work";
        return (int)command.ExecuteScalar();
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
