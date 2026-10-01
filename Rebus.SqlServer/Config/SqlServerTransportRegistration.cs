using Rebus.SqlServer;
using Rebus.SqlServer.Transport;

namespace Rebus.Config;

/// <summary>
/// Filled in when the SQL Server transport is created, so other SQL Server services can find the transport and its connection
/// provider even when <see cref="Rebus.Transport.ITransport"/> is decorated
/// </summary>
class SqlServerTransportRegistration
{
    public SqlServerTransport Transport { get; set; }

    public IDbConnectionProvider ConnectionProvider { get; set; }

    public bool IsOneWayClient { get; set; }
}
