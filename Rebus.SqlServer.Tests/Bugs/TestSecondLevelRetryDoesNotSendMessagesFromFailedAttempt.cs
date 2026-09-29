using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using Rebus.Activation;
using Rebus.Bus;
using Rebus.Config;
using Rebus.Exceptions;
using Rebus.Pipeline;
using Rebus.Retry.Simple;
using Rebus.Routing.TypeBased;
using Rebus.Tests.Contracts;
using Rebus.Transport;
// ReSharper disable AccessToDisposedClosure
#pragma warning disable CS1998

namespace Rebus.SqlServer.Tests.Bugs;

[TestFixture, Category(Categories.SqlServer)]
public class TestSecondLevelRetryDoesNotSendMessagesFromFailedAttempt : SecondLevelRetryOutgoingMessagesFixtureBase
{
    protected override void ConfigureTransport(StandardConfigurer<ITransport> configurer, string queueName) =>
        configurer.UseSqlServer(new SqlServerTransportOptions(SqlTestHelper.ConnectionString), queueName);

    [Test]
    [Description("Sends must be written in the receive's SQL transaction: if that transaction does not commit, neither the receive nor the sends may take effect")]
    public async Task SendsAreRolledBackTogetherWithReceiveWhenCommitFails()
    {
        var probes = new ConcurrentQueue<string>();
        var attempts = 0;
        var activator = Using(new BuiltinHandlerActivator());

        activator.Handle<Work>(async (bus, context, _) =>
        {
            var attempt = Interlocked.Increment(ref attempts);

            await bus.SendLocal(new Probe($"Send:{attempt}"));

            if (attempt == 1)
            {
                // runs after the transport has flushed its outgoing messages, but before the SQL transaction is committed
                context.TransactionContext.OnCommit(async _ => throw new InvalidOperationException("commit fails after sends were written"));
            }
        });

        activator.Handle<Probe>(async probe => probes.Enqueue(probe.Id));

        ConfigureBus(activator, maxDeliveryAttempts: 5);

        await activator.Bus.SendLocal(new Work());

        await WaitForProbes(probes, 1);

        Assert.That(probes, Is.EquivalentTo(new[] { "Send:2" }));
        Assert.That(attempts, Is.EqualTo(2));
    }

    [TestCase(true)]
    [TestCase(false)]
    [Description("Idha's NPoco OnTransactionCommit callbacks run in OnAck, after the SQL transaction has committed, and may send messages")]
    public async Task MessagesSentFromTransactionContextCallbacksAreDelivered(bool handlerSendsToo)
    {
        var probes = new ConcurrentQueue<string>();
        var activator = Using(new BuiltinHandlerActivator());

        activator.Handle<Work>(async (bus, context, _) =>
        {
            if (handlerSendsToo)
            {
                await bus.SendLocal(new Probe("handler"));
            }

            context.TransactionContext.OnCommit(async _ => await bus.SendLocal(new Probe("on-commit")));
            context.TransactionContext.OnAck(async _ => await bus.SendLocal(new Probe("on-ack")));
        });

        activator.Handle<Probe>(async probe => probes.Enqueue(probe.Id));

        ConfigureBus(activator, maxDeliveryAttempts: 5);

        await activator.Bus.SendLocal(new Work());

        var expectedProbes = handlerSendsToo ? new[] { "handler", "on-commit", "on-ack" } : new[] { "on-commit", "on-ack" };

        await WaitForProbes(probes, expectedProbes.Length);

        Assert.That(probes, Is.EquivalentTo(expectedProbes));
    }
}

[TestFixture, Category(Categories.SqlServer)]
public class TestSecondLevelRetryDoesNotSendMessagesFromFailedAttempt_LeaseTransport : SecondLevelRetryOutgoingMessagesFixtureBase
{
    protected override void ConfigureTransport(StandardConfigurer<ITransport> configurer, string queueName) =>
        configurer.UseSqlServerInLeaseMode(new SqlServerLeaseTransportOptions(SqlTestHelper.ConnectionString), queueName);
}

[Description(@"When a handler attempt ends on the 2nd level retry path, Rebus clears the transport's outgoing messages and then commits AND acks.
The messages sent by the failed attempt must not be delivered, whereas the messages sent by the IFailed<T> handler must be.")]
public abstract class SecondLevelRetryOutgoingMessagesFixtureBase : FixtureBase
{
    protected static readonly TimeSpan ProbeDeferral = TimeSpan.FromMilliseconds(300);

    string _queueName;
    string _subscriptionsTableName;

    protected override void SetUp()
    {
        base.SetUp();

        _queueName = TestConfig.GetName("slr-input");
        _subscriptionsTableName = TestConfig.GetName("slr-subscriptions");

        SqlTestHelper.DropTable(_queueName);
        SqlTestHelper.DropTable(_subscriptionsTableName);

        Using(new DisposableCallback(() => SqlTestHelper.DropTable(_queueName)));
        Using(new DisposableCallback(() => SqlTestHelper.DropTable(_subscriptionsTableName)));
    }

    public enum SendKind { Send, SendLocal, DeferLocal, Publish }

    [TestCase(SendKind.Send, 1, 1)]
    [TestCase(SendKind.SendLocal, 1, 1)]
    [TestCase(SendKind.DeferLocal, 1, 1)]
    [TestCase(SendKind.Publish, 1, 1)]
    [TestCase(SendKind.DeferLocal, 5, 5)]
    public async Task MessagesFromAttemptThatEndsOnSecondLevelPathAreNotDelivered(SendKind kind, int maxDeliveryAttempts, int failures)
    {
        await RunScenario(kind, maxDeliveryAttempts, failures, attempt => new InvalidOperationException($"attempt {attempt} fails"),
            expectedProbes: [$"{kind}:{failures + 1}", "failed-handler"]);
    }

    [TestCase(SendKind.Send)]
    [TestCase(SendKind.SendLocal)]
    [TestCase(SendKind.DeferLocal)]
    [TestCase(SendKind.Publish)]
    public async Task MessagesFromAttemptThatFailsFastIntoSecondLevelAreNotDelivered(SendKind kind)
    {
        await RunScenario(kind, maxDeliveryAttempts: 5, failures: 1, attempt => new FailFastException($"attempt {attempt} fails fast"),
            expectedProbes: [$"{kind}:2", "failed-handler"]);
    }

    [Test]
    public async Task FirstLevelFailureStillRollsBackMessagesFromFailedAttempt()
    {
        await RunScenario(SendKind.DeferLocal, maxDeliveryAttempts: 5, failures: 1, attempt => new InvalidOperationException($"attempt {attempt} fails"),
            expectedProbes: ["DeferLocal:2"]);
    }

    [Test]
    public async Task MessagesFromSuccessfulHandlerAreDelivered()
    {
        await RunScenario(SendKind.Send, maxDeliveryAttempts: 5, failures: 0, _ => new InvalidOperationException("never thrown"),
            expectedProbes: ["Send:1"]);
    }

    [Test]
    public async Task MessagesSentOutsideOfHandlerAreDelivered()
    {
        var probes = new ConcurrentQueue<string>();
        var activator = Using(new BuiltinHandlerActivator());

        activator.Handle<Probe>(async probe => probes.Enqueue(probe.Id));

        ConfigureBus(activator, maxDeliveryAttempts: 5);

        var bus = activator.Bus;

        await bus.SendLocal(new Probe("no-scope"));
        await bus.DeferLocal(ProbeDeferral, new Probe("deferred-no-scope"));

        using (var scope = new RebusTransactionScope())
        {
            await bus.SendLocal(new Probe("completed-scope"));
            await scope.CompleteAsync();
        }

        using (new RebusTransactionScope())
        {
            await bus.SendLocal(new Probe("abandoned-scope"));
        }

        await WaitForProbes(probes, 3);

        Assert.That(probes, Is.EquivalentTo(new[] { "no-scope", "deferred-no-scope", "completed-scope" }));
    }

    async Task RunScenario(SendKind kind, int maxDeliveryAttempts, int failures, Func<int, Exception> getException, string[] expectedProbes)
    {
        var probes = new ConcurrentQueue<string>();
        var attempts = 0;
        var activator = Using(new BuiltinHandlerActivator());

        activator.Handle<Work>(async (bus, _) =>
        {
            var attempt = Interlocked.Increment(ref attempts);

            await SendProbe(bus, kind, new Probe($"{kind}:{attempt}"));

            if (attempt <= failures) throw getException(attempt);
        });

        activator.Handle<IFailed<Work>>(async (bus, _) =>
        {
            await bus.SendLocal(new Probe("failed-handler"));
            await bus.Advanced.TransportMessage.Defer(ProbeDeferral);
        });

        activator.Handle<Probe>(async probe => probes.Enqueue(probe.Id));

        ConfigureBus(activator, maxDeliveryAttempts);

        if (kind == SendKind.Publish)
        {
            await activator.Bus.Subscribe<Probe>();
        }

        await activator.Bus.SendLocal(new Work());

        await WaitForProbes(probes, expectedProbes.Length);

        Assert.That(probes, Is.EquivalentTo(expectedProbes));
        Assert.That(attempts, Is.EqualTo(failures + 1), "Expected the Work message to succeed on the attempt after the last failure");
    }

    static Task SendProbe(IBus bus, SendKind kind, Probe probe) => kind switch
    {
        SendKind.Send => bus.Send(probe),
        SendKind.SendLocal => bus.SendLocal(probe),
        SendKind.DeferLocal => bus.DeferLocal(ProbeDeferral, probe),
        SendKind.Publish => bus.Publish(probe),
        _ => throw new ArgumentOutOfRangeException(nameof(kind), kind, null)
    };

    protected abstract void ConfigureTransport(StandardConfigurer<ITransport> configurer, string queueName);

    protected void ConfigureBus(BuiltinHandlerActivator activator, int maxDeliveryAttempts)
    {
        Configure.With(activator)
            .Transport(t => ConfigureTransport(t, _queueName))
            .Subscriptions(s => s.StoreInSqlServer(SqlTestHelper.ConnectionString, _subscriptionsTableName, isCentralized: true))
            .Routing(r => r.TypeBased().Map<Probe>(_queueName))
            .Options(o =>
            {
                o.SetNumberOfWorkers(1);
                o.SetMaxParallelism(1);
                o.RetryStrategy(maxDeliveryAttempts: maxDeliveryAttempts, secondLevelRetriesEnabled: true);
            })
            .Start();
    }

    // waits for the expected number of probes, then a little longer to give leaked (possibly deferred) probes a chance to show up
    protected static async Task WaitForProbes(ConcurrentQueue<string> probes, int expectedCount)
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));

        while (probes.Count < expectedCount)
        {
            await Task.Delay(50, timeout.Token);
        }

        await Task.Delay(ProbeDeferral + TimeSpan.FromSeconds(2));
    }

    protected record Work;

    protected record Probe(string Id);
}
