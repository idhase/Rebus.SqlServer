using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using NUnit.Framework;
using Rebus.Activation;
using Rebus.Bus;
using Rebus.Config;
using Rebus.Messages;
using Rebus.Retry.Simple;
using Rebus.Serialization;
using Rebus.SqlServer.Transport;
using Rebus.Tests.Contracts;
using Rebus.Transport;
// ReSharper disable AccessToDisposedClosure
#pragma warning disable CS1998

namespace Rebus.SqlServer.Tests.Transport;

[TestFixture(false)]
[TestFixture(true)]
[Category(Categories.SqlServer)]
[Description("Messages sent from a handler are inserted with one multi-row INSERT per destination table and power-of-two chunk of at most 32 messages")]
public class TestSqlServerTransportBatchedSends : FixtureBase
{
    const string SequenceHeader = "test-seq";

    static readonly HeaderSerializer HeaderSerializer = new();

    readonly bool _leaseMode;

    string _inputQueueName;
    string _destinationA;
    string _destinationB;
    string _errorQueueName;
    string _insertLogTableName;

    public TestSqlServerTransportBatchedSends(bool leaseMode) => _leaseMode = leaseMode;

    protected override void SetUp()
    {
        base.SetUp();

        _inputQueueName = TestConfig.GetName("batch-input");
        _destinationA = TestConfig.GetName("batch-destination-a");
        _destinationB = TestConfig.GetName("batch-destination-b");
        _errorQueueName = TestConfig.GetName("batch-error");
        _insertLogTableName = TestConfig.GetName("batch-insert-log");

        foreach (var table in new[] { _inputQueueName, _destinationA, _destinationB, _errorQueueName, _insertLogTableName })
        {
            SqlTestHelper.DropTable(table);
            Using(new DisposableCallback(() => SqlTestHelper.DropTable(table)));
        }

        SqlTestHelper.Execute($"CREATE TABLE [dbo].[{_insertLogTableName}] ([id] int IDENTITY(1,1) NOT NULL PRIMARY KEY, [table] nvarchar(200) NOT NULL, [rows] int NOT NULL)");

        CreateQueueWithInsertLog(_destinationA);
        CreateQueueWithInsertLog(_destinationB);
    }

    [TestCase(1, new[] { 1 })]
    [TestCase(5, new[] { 4, 1 })]
    [TestCase(20, new[] { 16, 4 })]
    [TestCase(32, new[] { 32 })]
    [TestCase(45, new[] { 32, 8, 4, 1 })]
    public async Task InsertsMessagesInPowerOfTwoChunksOfAtMost32(int count, int[] expectedChunkSizes)
    {
        await SendFromHandler(bus => SendSequence(bus, Enumerable.Range(0, count).Select(n => (_destinationA, n))));

        await WaitForRowCount(_destinationA, count);

        Assert.That(GetInsertLog(_destinationA), Is.EqualTo(expectedChunkSizes));
        Assert.That(GetSequenceNumbers(_destinationA), Is.EqualTo(Enumerable.Range(0, count)));
    }

    [Test]
    [Description("7 parameters per message: more than 300 messages in one command would exceed SQL Server's limit of 2100 parameters")]
    public async Task SendsMoreMessagesThanFitInOneCommand()
    {
        const int count = 700;

        await SendFromHandler(bus => SendSequence(bus, Enumerable.Range(0, count).Select(n => (_destinationA, n))));

        await WaitForRowCount(_destinationA, count);

        Assert.That(GetInsertLog(_destinationA), Is.EqualTo(Enumerable.Repeat(32, 21).Concat([16, 8, 4])));
        Assert.That(GetSequenceNumbers(_destinationA), Is.EqualTo(Enumerable.Range(0, count)));
    }

    [Test]
    public async Task GroupsMessagesByDestinationAndKeepsTheirOrder()
    {
        var destinations = new[] { _destinationA, _destinationB, _destinationA, _destinationB, _destinationA, _destinationA, _destinationB };

        await SendFromHandler(bus => SendSequence(bus, destinations.Select((destination, n) => (destination, n))));

        await WaitForRowCount(_destinationA, 4);
        await WaitForRowCount(_destinationB, 3);

        Assert.That(GetInsertLog(_destinationA), Is.EqualTo(new[] { 4 }));
        Assert.That(GetInsertLog(_destinationB), Is.EqualTo(new[] { 2, 1 }));
        Assert.That(GetSequenceNumbers(_destinationA), Is.EqualTo(new[] { 0, 2, 4, 5 }));
        Assert.That(GetSequenceNumbers(_destinationB), Is.EqualTo(new[] { 1, 3, 6 }));
    }

    [Test]
    public async Task KeepsPriorityDeferralAndTimeToBeReceivedOfEachMessage()
    {
        await SendFromHandler(async bus =>
        {
            await bus.Advanced.Routing.Send(_destinationA, "plain", new Dictionary<string, string> { [SequenceHeader] = "0" });
            await bus.Advanced.Routing.Send(_destinationA, "prioritized", new Dictionary<string, string> { [SequenceHeader] = "1", [SqlServerTransport.MessagePriorityHeaderKey] = "5" });
            await bus.Advanced.Routing.Defer(_destinationA, TimeSpan.FromHours(1), "deferred", new Dictionary<string, string> { [SequenceHeader] = "2" });
            await bus.Advanced.Routing.Send(_destinationA, "expiring", new Dictionary<string, string> { [SequenceHeader] = "3", [Headers.TimeToBeReceived] = "00:10:00" });
        });

        await WaitForRowCount(_destinationA, 4);

        var rows = GetRows(_destinationA).ToDictionary(r => r.Headers[SequenceHeader]);

        Assert.That(GetInsertLog(_destinationA), Is.EqualTo(new[] { 4 }));

        Assert.That(rows["0"].Priority, Is.EqualTo(0));
        Assert.That(rows["1"].Priority, Is.EqualTo(5));

        Assert.That(rows["0"].VisibleInSeconds, Is.LessThanOrEqualTo(0));
        Assert.That(rows["2"].VisibleInSeconds, Is.InRange(3500, 3600));
        Assert.That(rows["2"].Headers.ContainsKey(Headers.DeferredUntil), Is.False, "The deferred-until header must be removed, because the visible column takes care of the deferral");

        Assert.That(rows["3"].ExpiresInSeconds, Is.InRange(500, 600));
        Assert.That(rows["0"].ExpiresInSeconds, Is.GreaterThan(TimeSpan.FromDays(3650).TotalSeconds));
    }

    [Test]
    public async Task FailingInsertRollsBackAllMessagesOfTheHandler()
    {
        SqlTestHelper.Execute($@"
CREATE TRIGGER [dbo].[Fail_{_destinationB}] ON [dbo].[{_destinationB}]
AFTER INSERT
AS
BEGIN
    THROW 51000, 'inserting into {_destinationB} fails on purpose', 1;
END");

        await SendFromHandler(bus => SendSequence(bus, new[] { (_destinationA, 0), (_destinationA, 1), (_destinationA, 2), (_destinationB, 3) }), maxDeliveryAttempts: 1);

        await WaitForRowCount(_errorQueueName, 1);

        Assert.That(CountRows(_destinationA), Is.Zero);
    }

    async Task SendFromHandler(Func<IBus, Task> send, int maxDeliveryAttempts = 5)
    {
        var activator = Using(new BuiltinHandlerActivator());

        activator.Handle<string>(async (bus, message) =>
        {
            if (message != "go") return;

            await send(bus);
        });

        Configure.With(activator)
            .Transport(t => ConfigureTransport(t, _inputQueueName))
            .Options(o =>
            {
                o.SetNumberOfWorkers(1);
                o.SetMaxParallelism(1);
                o.RetryStrategy(errorQueueName: _errorQueueName, maxDeliveryAttempts: maxDeliveryAttempts);
            })
            .Start();

        await activator.Bus.SendLocal("go");
    }

    static async Task SendSequence(IBus bus, IEnumerable<(string Destination, int Sequence)> messages)
    {
        foreach (var (destination, sequence) in messages)
        {
            await bus.Advanced.Routing.Send(destination, $"message {sequence}", new Dictionary<string, string> { [SequenceHeader] = sequence.ToString() });
        }
    }

    void ConfigureTransport(StandardConfigurer<ITransport> configurer, string queueName)
    {
        if (_leaseMode)
        {
            configurer.UseSqlServerInLeaseMode(new SqlServerLeaseTransportOptions(SqlTestHelper.ConnectionString), queueName);
        }
        else
        {
            configurer.UseSqlServer(new SqlServerTransportOptions(SqlTestHelper.ConnectionString), queueName);
        }
    }

    void CreateQueueWithInsertLog(string queueName)
    {
        var activator = Using(new BuiltinHandlerActivator());

        Configure.With(activator)
            .Transport(t => ConfigureTransport(t, queueName))
            .Options(o => o.SetNumberOfWorkers(0))
            .Start();

        // AFTER INSERT triggers fire once per INSERT statement, so this logs how many rows each statement inserted
        SqlTestHelper.Execute($@"
CREATE TRIGGER [dbo].[LogInserts_{queueName}] ON [dbo].[{queueName}]
AFTER INSERT
AS
BEGIN
    INSERT INTO [dbo].[{_insertLogTableName}] ([table], [rows]) SELECT '{queueName}', COUNT(*) FROM inserted
END");
    }

    List<int> GetInsertLog(string queueName) =>
        Query($"SELECT [rows] FROM [dbo].[{_insertLogTableName}] WHERE [table] = '{queueName}' ORDER BY [id]", r => r.GetInt32(0));

    List<int> GetSequenceNumbers(string queueName) =>
        GetRows(queueName).Select(r => int.Parse(r.Headers[SequenceHeader])).ToList();

    List<Row> GetRows(string queueName) =>
        Query($@"
SELECT  [headers], [priority], DATEDIFF(second, SYSDATETIMEOFFSET(), [visible]), DATEDIFF_BIG(second, SYSDATETIMEOFFSET(), [expiration])
FROM    [dbo].[{queueName}]
ORDER BY [id]", r => new Row(HeaderSerializer.Deserialize((byte[])r[0]), r.GetInt32(1), r.GetInt32(2), r.GetInt64(3)));

    int CountRows(string queueName) => Query($"SELECT COUNT(*) FROM [dbo].[{queueName}]", r => r.GetInt32(0)).Single();

    async Task WaitForRowCount(string queueName, int expectedCount)
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));

        while (CountRows(queueName) < expectedCount)
        {
            await Task.Delay(100, timeout.Token);
        }

        // give any extra, unexpected rows a chance to show up
        await Task.Delay(TimeSpan.FromSeconds(1));
    }

    static List<T> Query<T>(string sql, Func<SqlDataReader, T> map)
    {
        using var connection = new SqlConnection(SqlTestHelper.ConnectionString);
        connection.Open();

        using var command = new SqlCommand(sql, connection);
        using var reader = command.ExecuteReader();

        var results = new List<T>();

        while (reader.Read())
        {
            results.Add(map(reader));
        }

        return results;
    }

    record Row(Dictionary<string, string> Headers, int Priority, int VisibleInSeconds, long ExpiresInSeconds);
}
