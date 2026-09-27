using System.Collections.Generic;
using Microsoft.Data.SqlClient;
using NUnit.Framework;
using Rebus.Activation;
using Rebus.Config;
using Rebus.Tests.Contracts;

namespace Rebus.SqlServer.Tests.Transport;

[TestFixture, Category(Categories.SqlServer)]
public class TestSqlServerTransportReceiveIndex : FixtureBase
{
    string _queueName;

    protected override void SetUp()
    {
        base.SetUp();

        _queueName = TestConfig.GetName("receive-index");

        SqlTestHelper.DropTable(_queueName);

        Using(new DisposableCallback(() => SqlTestHelper.DropTable(_queueName)));
    }

    [Test]
    [Description("The receive query orders by [priority] DESC, [visible], [id], so the receive index must have the same key columns in the same order")]
    public void CreatesReceiveIndexMatchingTheReceiveQuery()
    {
        var activator = Using(new BuiltinHandlerActivator());

        Configure.With(activator)
            .Transport(t => t.UseSqlServer(new SqlServerTransportOptions(SqlTestHelper.ConnectionString), _queueName))
            .Options(o => o.SetNumberOfWorkers(0))
            .Start();

        var keyColumns = GetIndexKeyColumns($"dbo.{_queueName}", $"IDX_RECEIVE_dbo_{_queueName}");

        Assert.That(keyColumns, Is.EqualTo(new[] { "[priority] DESC", "[visible] ASC", "[id] ASC", "[expiration] ASC" }));
    }

    static List<string> GetIndexKeyColumns(string tableName, string indexName)
    {
        using var connection = new SqlConnection(SqlTestHelper.ConnectionString);
        connection.Open();

        using var command = connection.CreateCommand();
        command.CommandText = @"
SELECT      c.[name], ic.[is_descending_key]
FROM        sys.indexes i
JOIN        sys.index_columns ic ON ic.[object_id] = i.[object_id] AND ic.[index_id] = i.[index_id]
JOIN        sys.columns c ON c.[object_id] = ic.[object_id] AND c.[column_id] = ic.[column_id]
WHERE       i.[object_id] = OBJECT_ID(@tableName) AND i.[name] = @indexName AND ic.[key_ordinal] > 0
ORDER BY    ic.[key_ordinal]";
        command.Parameters.AddWithValue("@tableName", tableName);
        command.Parameters.AddWithValue("@indexName", indexName);

        var columns = new List<string>();

        using var reader = command.ExecuteReader();

        while (reader.Read())
        {
            columns.Add($"[{reader.GetString(0)}] {(reader.GetBoolean(1) ? "DESC" : "ASC")}");
        }

        return columns;
    }
}
