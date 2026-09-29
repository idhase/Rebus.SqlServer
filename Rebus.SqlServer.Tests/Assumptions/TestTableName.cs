using System;
using System.Text.RegularExpressions;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace Rebus.SqlServer.Tests.Assumptions;

[TestFixture]
public class TestTableName
{
    [TestCase("[bimse]", "[bimse]", true)]
    [TestCase("[bimse]", "[BIMSE]", true)]
    public void CheckEquality(string name1, string name2, bool expectedToBeEqual)
    {
        var tableName1 = TableName.Parse(name1);
        var tableName2 = TableName.Parse(name2);

        var what = expectedToBeEqual
            ? Is.EqualTo(tableName2)
            : Is.Not.EqualTo(tableName2);

            Assert.That(tableName1, what);
        }

        [TestCase("table].[schema")]
        [TestCase("table] .[schema")]
        [TestCase("table]  .[schema")]
        [TestCase("table]. [schema")]
        [TestCase("table].  [schema")]
        [TestCase("table].   [schema")]
        [TestCase("table] . [schema")]
        [TestCase("catalog]. [table] . [schema")]
        public void RegexSplitter(string text)
        {
            var partsThingie = Regex.Split(text, @"\][ ]*\.[ ]*\[");

            Console.WriteLine($"Found parts: {string.Join(", ", partsThingie)}");
        }


        [TestCase("table", "dbo", "table")]
        [TestCase("[table]", "dbo", "table")]
        [TestCase("dbo.table", "dbo", "table", Description = "Allow short-hand for this simple case to avoid having to use brackets")]
        [TestCase("schema.table", "schema", "table")]
        [TestCase("[schema].[table]", "schema", "table")]
        [TestCase("[Table name with spaces in it]", "dbo", "Table name with spaces in it")]
        [TestCase("[Table name with . in it]", "dbo", "Table name with . in it")]
        [TestCase("[schema-qualified table name with dots in it].[Table name with . in it]", "schema-qualified table name with dots in it", "Table name with . in it")]
        [TestCase("[Schema name with . in it].[Table name with . in it]", "Schema name with . in it", "Table name with . in it")]
        [TestCase("[Schema name with . in it] .[Table name with . in it]", "Schema name with . in it", "Table name with . in it")]
        [TestCase("[Schema name with . in it] . [Table name with . in it]", "Schema name with . in it", "Table name with . in it")]
        [TestCase("[Schema name with . in it]. [Table name with . in it]", "Schema name with . in it", "Table name with . in it")]
        [TestCase("[catalog name with . in it]. [Schema name with . in it]. [Table name with . in it]", "Schema name with . in it", "Table name with . in it", "catalog name with . in it")]
        public void MoreExamples(string input, string expectedSchema, string expectedTable, string expectedCatalog = null)
        {
            var tableName = TableName.Parse(input);

            Assert.That(tableName.Schema, Is.EqualTo(expectedSchema));
            Assert.That(tableName.Name, Is.EqualTo(expectedTable));
            Assert.That(tableName.Catalog, Is.EqualTo(expectedCatalog));
            Assert.That(tableName.QualifiedName,  expectedCatalog == null ? Is.EqualTo($"[{expectedSchema}].[{expectedTable}]") : Is.EqualTo($"[{expectedCatalog}].[{expectedSchema}].[{expectedTable}]"));
        }

        [Test]
        public void ParsesNameWithoutSchemaAssumingDboAsDefault()
        {
            var table = TableName.Parse("TableName");

            ClassicAssert.AreEqual(table.Name, "TableName");
            ClassicAssert.AreEqual(table.Schema, "dbo");
            ClassicAssert.IsNull(table.Catalog);
            ClassicAssert.AreEqual(table.QualifiedName, "[dbo].[TableName]");
        }

        [Test]
        public void ParsesBracketsNameWithoutSchemaAssumingDboAsDefault()
        {
            var table = TableName.Parse("[TableName]");

            ClassicAssert.AreEqual(table.Name, "TableName");
            ClassicAssert.AreEqual(table.Schema, "dbo");
            ClassicAssert.IsNull(table.Catalog);
            ClassicAssert.AreEqual(table.QualifiedName, "[dbo].[TableName]");
        }

        [Test]
        public void ParsesNameWithSchema()
        {
            var table = TableName.Parse("schema.TableName");

            ClassicAssert.AreEqual(table.Name, "TableName");
            ClassicAssert.AreEqual(table.Schema, "schema");
            ClassicAssert.IsNull(table.Catalog);
            ClassicAssert.AreEqual(table.QualifiedName, "[schema].[TableName]");
        }

        [Test]
        public void ParsesBracketsNameWithSchema()
        {
            var table = TableName.Parse("[schema].[TableName]");

            ClassicAssert.AreEqual(table.Name, "TableName");
            ClassicAssert.AreEqual(table.Schema, "schema");
            ClassicAssert.IsNull(table.Catalog);
            ClassicAssert.AreEqual(table.QualifiedName, "[schema].[TableName]");
        }

        [Test]
        public void ParsesNameWithSchemaAndCatalog()
        {
            var table = TableName.Parse("catalog.schema.TableName");

            ClassicAssert.AreEqual(table.Name, "TableName");
            ClassicAssert.AreEqual(table.Schema, "schema");
            ClassicAssert.AreEqual(table.Catalog, "catalog");
            ClassicAssert.AreEqual(table.QualifiedName, "[catalog].[schema].[TableName]");
        }

        [Test]
        public void ParsesBracketsNameWithSchemaAndCatalog()
        {
            var table = TableName.Parse("[catalog].[schema].[TableName]");

            ClassicAssert.AreEqual(table.Name, "TableName");
            ClassicAssert.AreEqual(table.Schema, "schema");
            ClassicAssert.AreEqual(table.Catalog, "catalog");
            ClassicAssert.AreEqual(table.QualifiedName, "[catalog].[schema].[TableName]");
        }

        [Test]
        public void NullCatalogIsReportedAsCatalog()
        {
            var exception = Assert.Throws<ArgumentNullException>(() => new TableName("schema", "TableName", null));

            Assert.That(exception.ParamName, Is.EqualTo("catalog"));
        }
    

    //[TestCase("table].[schema")]
    //[TestCase("table] .[schema")]
    //[TestCase("table]  .[schema")]
    //[TestCase("table]. [schema")]
    //[TestCase("table].  [schema")]
    //[TestCase("table].   [schema")]
    //[TestCase("table] . [schema")]
    //public void RegexSplitter(string text)
    //{
    //    var partsThingie = Regex.Split(text, @"\][ ]*\.[ ]*\[");

    //    Console.WriteLine($"Found parts: {string.Join(", ", partsThingie)}");
    //}


    //[TestCase("table", "dbo", "table")]
    //[TestCase("[table]", "dbo", "table")]
    //[TestCase("dbo.table", "dbo", "table")]
    //[TestCase("schema.table", "schema", "table")]
    //[TestCase("[schema].[table]", "schema", "table")]
    //[TestCase("[Table name with spaces in it]", "dbo", "Table name with spaces in it")]
    //[TestCase("[Table name with . in it]", "dbo", "Table name with . in it")]
    //[TestCase("[schema-qualified table name with dots in it].[Table name with . in it]", "schema-qualified table name with dots in it", "Table name with . in it")]
    //[TestCase("[Schema name with . in it].[Table name with . in it]", "Schema name with . in it", "Table name with . in it")]
    //[TestCase("[Schema name with . in it] .[Table name with . in it]", "Schema name with . in it", "Table name with . in it")]
    //[TestCase("[Schema name with . in it] . [Table name with . in it]", "Schema name with . in it", "Table name with . in it")]
    //[TestCase("[Schema name with . in it]. [Table name with . in it]", "Schema name with . in it", "Table name with . in it")]
    //public void MoreExamples(string input, string expectedSchema, string expectedTable)
    //{
    //    var tableName = TableName.Parse(input);

    //    Assert.That(tableName.Schema, Is.EqualTo(expectedSchema));
    //    Assert.That(tableName.Name, Is.EqualTo(expectedTable));
    //    Assert.That(tableName.QualifiedName, Is.EqualTo($"[{expectedSchema}].[{expectedTable}]"));
    //}

    //[Test]
    //public void ParsesNameWithoutSchemaAssumingDboAsDefault()
    //{
    //    var table = TableName.Parse("TableName");

    //    Assert.That(table.Name, Is.EqualTo("TableName"));
    //    Assert.That(table.Schema, Is.EqualTo("dbo"));
    //    Assert.That(table.QualifiedName, Is.EqualTo("[dbo].[TableName]"));
    //}

    //[Test]
    //public void ParsesBracketsNameWithoutSchemaAssumingDboAsDefault()
    //{
    //    var table = TableName.Parse("[TableName]");

    //    Assert.That(table.Name, Is.EqualTo("TableName"));
    //    Assert.That(table.Schema, Is.EqualTo("dbo"));
    //    Assert.That(table.QualifiedName, Is.EqualTo("[dbo].[TableName]"));
    //}

    //[Test]
    //public void ParsesNameWithSingleDotAsSchema()
    //{
    //    var table = TableName.Parse("schema.TableName");

    //    Assert.That(table.Name, Is.EqualTo("TableName"));
    //    Assert.That(table.Schema, Is.EqualTo("schema"));
    //    Assert.That(table.QualifiedName, Is.EqualTo("[schema].[TableName]"));
    //}

    //[Test]
    //public void ParsesNameWithMultipleDotsAsTableName()
    //{
    //    var table = TableName.Parse("notschema.alsonotschema.TableName");

    //    Assert.That(table.Name, Is.EqualTo("notschema.alsonotschema.TableName"));
    //    Assert.That(table.Schema, Is.EqualTo("dbo"));
    //    Assert.That(table.QualifiedName, Is.EqualTo("[dbo].[notschema.alsonotschema.TableName]"));
    //}

    //[Test]
    //public void ParsesBracketsNameWithSchema()
    //{
    //    var table = TableName.Parse("[schema].[TableName]");

    //    Assert.That(table.Name, Is.EqualTo("TableName"));
    //    Assert.That(table.Schema, Is.EqualTo("schema"));
    //    Assert.That(table.QualifiedName, Is.EqualTo("[schema].[TableName]"));
    //}
}