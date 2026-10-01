using System;
using System.Threading.Tasks;
using Rebus.Exceptions;
using Rebus.Injection;
using Rebus.Logging;
using Rebus.Sagas;
using Rebus.SqlServer;
using Rebus.SqlServer.Sagas;
using Rebus.SqlServer.Sagas.Serialization;
using Rebus.SqlServer.Transport;
using Rebus.Transport;

namespace Rebus.Config;

/// <summary>
/// Configuration extensions for sagas
/// </summary>
public static class SqlServerSagaConfigurationExtensions
{
    /// <summary>
    /// Configures Rebus to use SQL Server to store sagas, using the tables specified to store data and indexed properties respectively.
    /// </summary>
    public static void StoreInSqlServer(this StandardConfigurer<ISagaStorage> configurer,
        string connectionString, string dataTableName, string indexTableName,
        bool automaticallyCreateTables = true, bool enlistInAmbientTransaction = false)
    {
        if (configurer == null) throw new ArgumentNullException(nameof(configurer));
        if (connectionString == null) throw new ArgumentNullException(nameof(connectionString));
        if (dataTableName == null) throw new ArgumentNullException(nameof(dataTableName));
        if (indexTableName == null) throw new ArgumentNullException(nameof(indexTableName));

        configurer.Register(c =>
        {
            var rebusLoggerFactory = c.Get<IRebusLoggerFactory>();
            var connectionProvider = new DbConnectionProvider(connectionString, rebusLoggerFactory, enlistInAmbientTransaction);
            var sagaTypeNamingStrategy = GetSagaTypeNamingStrategy(c, rebusLoggerFactory);
            var serializer = c.Has<ISagaSerializer>(false) ? c.Get<ISagaSerializer>() : new DefaultSagaSerializer();

            var sagaStorage = new SqlServerSagaStorage(connectionProvider, dataTableName, indexTableName, rebusLoggerFactory, sagaTypeNamingStrategy, serializer);

            if (automaticallyCreateTables)
            {
                sagaStorage.EnsureTablesAreCreated();
            }

            return sagaStorage;
        });
    }

    /// <summary>
    /// Configures Rebus to use SQL Server to store sagas, using the tables specified to store data and indexed properties respectively.
    /// </summary>
    public static void StoreInSqlServer(this StandardConfigurer<ISagaStorage> configurer,
        Func<Task<IDbConnection>> connectionFactory, string dataTableName, string indexTableName,
        bool automaticallyCreateTables = true)
    {
        if (configurer == null) throw new ArgumentNullException(nameof(configurer));
        if (connectionFactory == null) throw new ArgumentNullException(nameof(connectionFactory));
        if (dataTableName == null) throw new ArgumentNullException(nameof(dataTableName));
        if (indexTableName == null) throw new ArgumentNullException(nameof(indexTableName));

        configurer.Register(c =>
        {
            var rebusLoggerFactory = c.Get<IRebusLoggerFactory>();
            var connectionProvider = new DbConnectionFactoryProvider(connectionFactory);
            var sagaTypeNamingStrategy = GetSagaTypeNamingStrategy(c, rebusLoggerFactory);
            var serializer = c.Has<ISagaSerializer>(false) ? c.Get<ISagaSerializer>() : new DefaultSagaSerializer();

            var sagaStorage = new SqlServerSagaStorage(connectionProvider, dataTableName, indexTableName, rebusLoggerFactory, sagaTypeNamingStrategy, serializer);

            if (automaticallyCreateTables)
            {
                sagaStorage.EnsureTablesAreCreated();
            }

            return sagaStorage;
        });
    }

    /// <summary>
    /// Configures Rebus to use SQL Server to store sagas, using the tables specified to store data and indexed properties respectively.
    /// Saga data is stored in the transport's database: while a message is being handled it is read and written through the
    /// SQL Server transport's connection and transaction, so it commits and rolls back together with the receive and the
    /// outgoing messages. Outside of message handling, e.g. when creating the tables, the transport's connection provider is used.
    /// Requires the (non-lease) SQL Server transport working in a SqlTransaction of its own, i.e. not configured with
    /// enlistInAmbientTransaction: true, and throws at startup otherwise.
    /// </summary>
    public static void StoreInSqlServerUsingTransportConnection(this StandardConfigurer<ISagaStorage> configurer,
        string dataTableName, string indexTableName, bool automaticallyCreateTables = true)
    {
        if (configurer == null) throw new ArgumentNullException(nameof(configurer));
        if (dataTableName == null) throw new ArgumentNullException(nameof(dataTableName));
        if (indexTableName == null) throw new ArgumentNullException(nameof(indexTableName));

        configurer.Register(c =>
        {
            if (!c.Has<SqlServerTransportRegistration>())
            {
                throw new RebusConfigurationException($"{nameof(StoreInSqlServerUsingTransportConnection)} requires the SQL Server transport (UseSqlServer)");
            }

            // resolving the transport creates it, which fills in the registration
            c.Get<ITransport>();

            var registration = c.Get<SqlServerTransportRegistration>();

            if (registration.Transport is SqlServerLeaseTransport)
            {
                throw new RebusConfigurationException($"{nameof(StoreInSqlServerUsingTransportConnection)} does not work with the lease-based SQL Server transport, because it receives on a connection of its own");
            }

            if (registration.IsOneWayClient)
            {
                throw new RebusConfigurationException($"{nameof(StoreInSqlServerUsingTransportConnection)} does not work with a one-way client, which handles no messages and so has no sagas");
            }

            EnsureTransportWorksInASqlTransaction(registration.ConnectionProvider);

            var rebusLoggerFactory = c.Get<IRebusLoggerFactory>();
            var connectionProvider = new SqlServerTransportConnectionProvider(registration.ConnectionProvider);
            var sagaTypeNamingStrategy = GetSagaTypeNamingStrategy(c, rebusLoggerFactory);
            var serializer = c.Has<ISagaSerializer>(false) ? c.Get<ISagaSerializer>() : new DefaultSagaSerializer();

            var sagaStorage = new SqlServerSagaStorage(connectionProvider, dataTableName, indexTableName, rebusLoggerFactory, sagaTypeNamingStrategy, serializer);

            if (automaticallyCreateTables)
            {
                sagaStorage.EnsureTablesAreCreated();
            }

            return sagaStorage;
        });
    }

    // Saga writes are made all-or-nothing with savepoints in the transport's SqlTransaction. A transport that enlists in an ambient
    // System.Transactions transaction instead, or a connection factory that begins none, would fail every saga access at runtime,
    // so one connection is taken here to fail at startup instead. Disposing it uncompleted rolls back its empty transaction.
    static void EnsureTransportWorksInASqlTransaction(IDbConnectionProvider transportConnectionProvider)
    {
        using var connection = AsyncHelpers.GetSync(transportConnectionProvider.GetConnection);

        if (connection.Transaction == null)
        {
            throw new RebusConfigurationException($"{nameof(StoreInSqlServerUsingTransportConnection)} requires the SQL Server transport to work in a SqlTransaction of its own, which it doesn't when it's configured with enlistInAmbientTransaction: true, or with a connection factory that returns a connection without a transaction");
        }
    }

    /// <summary>
    /// Configures Rebus to use SQL Server to store sagas, using the tables specified to store data and indexed properties respectively.
    /// </summary>
    public static void StoreInSqlServer(this StandardConfigurer<ISagaStorage> configurer, SqlServerSagaStorageOptions options, string dataTableName, string indexTableName)
    {
        if (configurer == null) throw new ArgumentNullException(nameof(configurer));
        if (options == null) throw new ArgumentNullException(nameof(options));
        if (dataTableName == null) throw new ArgumentNullException(nameof(dataTableName));
        if (indexTableName == null) throw new ArgumentNullException(nameof(indexTableName));

        configurer.Register(c =>
        {
            var rebusLoggerFactory = c.Get<IRebusLoggerFactory>();
            var connectionProvider = options.ConnectionProviderFactory(c);
            var sagaTypeNamingStrategy = GetSagaTypeNamingStrategy(c, rebusLoggerFactory);
            var serializer = c.Has<ISagaSerializer>(false) ? c.Get<ISagaSerializer>() : new DefaultSagaSerializer();

            var sagaStorage = new SqlServerSagaStorage(
                connectionProvider: connectionProvider,
                dataTableName: dataTableName,
                indexTableName: indexTableName,
                rebusLoggerFactory: rebusLoggerFactory,
                sagaTypeNamingStrategy: sagaTypeNamingStrategy,
                sagaSerializer: serializer
            );

            if (options.EnsureTablesAreCreated)
            {
                sagaStorage.EnsureTablesAreCreated();
            }

            return sagaStorage;
        });
    }

    /// <summary>
    /// Configures saga to use your own custom saga serializer
    /// </summary>
    public static void UseSagaSerializer(this StandardConfigurer<ISagaStorage> configurer, ISagaSerializer serializer = null)
    {
        if (configurer == null) throw new ArgumentNullException(nameof(configurer));

        var serializerInstance = serializer ?? new DefaultSagaSerializer();

        configurer.OtherService<ISagaSerializer>().Decorate(c => serializerInstance);
    }

    /// <summary>
    /// Get the registered implementation of <seealso cref="ISagaTypeNamingStrategy"/> or the default <seealso cref="LegacySagaTypeNamingStrategy"/> if one is not configured
    /// </summary>
    static ISagaTypeNamingStrategy GetSagaTypeNamingStrategy(IResolutionContext resolutionContext, IRebusLoggerFactory rebusLoggerFactory)
    {
        if (resolutionContext.Has<ISagaTypeNamingStrategy>())
        {
            return resolutionContext.Get<ISagaTypeNamingStrategy>();
        }

        var logger = rebusLoggerFactory.GetLogger<SqlServerSagaStorage>();

        logger.Debug($"An implementation of {nameof(ISagaTypeNamingStrategy)} was not registered. A default, backward compatible, implementation will be used ({nameof(LegacySagaTypeNamingStrategy)}).");

        return new LegacySagaTypeNamingStrategy();
    }
}