using System;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using NUnit.Framework;
using Rebus.Activation;
using Rebus.Config;
using Rebus.Tests.Contracts;

namespace Rebus.SqlServer.Tests.Transport;

[TestFixture, Category(Categories.SqlServer)]
public class TestSqlServerTransportConcurrentSends : FixtureBase
{
    const int ParallelSends = 4;
    static readonly TimeSpan InsertDelay = TimeSpan.FromSeconds(1);

    string _queueName;

    protected override void SetUp()
    {
        base.SetUp();

        _queueName = TestConfig.GetName("concurrent-sends");

        SqlTestHelper.DropTable(_queueName);

        Using(new DisposableCallback(() => SqlTestHelper.DropTable(_queueName)));
    }

    [Test]
    [Description("Sends that use different connections must not wait for each other - access to each connection is already serialized by the ConnectionLocker")]
    public async Task SendsOnDifferentConnectionsRunConcurrently()
    {
        var receiver = Using(new BuiltinHandlerActivator());

        Configure.With(receiver)
            .Transport(t => t.UseSqlServer(new SqlServerTransportOptions(SqlTestHelper.ConnectionString), _queueName))
            .Options(o => o.SetNumberOfWorkers(0))
            .Start();

        SqlTestHelper.Execute($@"
CREATE TRIGGER [dbo].[SlowInsert_{_queueName}] ON [dbo].[{_queueName}]
AFTER INSERT
AS
BEGIN
    WAITFOR DELAY '00:00:0{InsertDelay.TotalSeconds}'
END");

        var sender = Using(new BuiltinHandlerActivator());

        var bus = Configure.With(sender)
            .Transport(t => t.UseSqlServerAsOneWayClient(new SqlServerTransportOptions(SqlTestHelper.ConnectionString)))
            .Start();

        var stopwatch = Stopwatch.StartNew();

        // each send happens outside of a handler, so each gets its own transaction context and thus its own connection
        await Task.WhenAll(Enumerable.Range(0, ParallelSends).Select(n => bus.Advanced.Routing.Send(_queueName, $"message {n}")));

        var elapsed = stopwatch.Elapsed;

        Console.WriteLine($"{ParallelSends} sends with a {InsertDelay.TotalSeconds} s INSERT delay took {elapsed.TotalSeconds:0.0} s");

        Assert.That(SqlTestHelper.Query<CountRow>($"SELECT COUNT(*) AS [Count] FROM [dbo].[{_queueName}]").Single().Count, Is.EqualTo(ParallelSends));
        Assert.That(elapsed, Is.LessThan(InsertDelay * (ParallelSends / 2.0)), "The sends were executed one at a time");
    }

    class CountRow
    {
        public int Count { get; set; }
    }
}
