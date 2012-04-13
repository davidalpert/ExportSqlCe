﻿    using System;
    using System.IO;
    using NUnit.Framework;
    using System.Data;
    using System.Data.SqlClient;
    using System.Data.SqlServerCe;
using ErikEJ.SqlCeScripting;


    [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Naming", "CA1709:IdentifiersShouldBeCasedCorrectly", MessageId = "Ce"), TestFixture]
    public class SqlCeScriptingTestFixture
    {
        private enum SchemaType
        { 
            NoConstraints,
            FullConstraints,
            FullNoIdentity,
            DataReaderTest
        }

        private const string sdfConnectionString = @"Data Source=C:\data\sqlce\test\ams40.sdf;Max Database Size=512";
        private const string serverConnectionString = @"data source=.\SQL2008R2;Initial Catalog=AdventureWorksLT2008R2;Integrated Security=true";
        private const string chinookConnectionString = @"Data Source=C:\projects\Chinook\Chinook40.sdf;";
        private const string migrateConnectionString = @"data source=.\SQL2008R2;Initial Catalog=MigrateTest;Integrated Security=true";


        [Test]
        public void TestServerMigration()
        {
            string path = @"C:\temp\testChinook40.sqlce";
            using (IRepository sourceRepository = new DB4Repository(chinookConnectionString))
            {
                var generator = new Generator4(sourceRepository, path);
                generator.GenerateAllAndSave(true, false, false);
            }
            Assert.IsTrue(System.IO.File.Exists(path));
            using (IRepository serverRepository = new ServerDBRepository4(migrateConnectionString))
            {
                serverRepository.ExecuteSqlFile(path);
            }
        }

        [Test]
        public void ExerciseEngineWithTable()
        {
            using (IRepository sourceRepository = new DB4Repository(sdfConnectionString))
            {
                var generator = new Generator4(sourceRepository);
                using (IRepository targetRepository = new ServerDBRepository4(serverConnectionString))
                {
                    SqlCeDiff.CreateDiffScript(sourceRepository, targetRepository, generator, false);
                }
            }
        }

        [Test]
        public void TestSqlText()
        {
            string sql = @"-- Script Date: 13-03-2012 20:03  - Generated by ExportSqlCe version 3.5.2.7
-- Database information:
-- Locale Identifier: 1030
-- Encryption Mode: 
-- Case Sensitive: False
-- Database: C:\data\sqlce\test\nw40.sdf
-- ServerVersion: 4.0.8854.1
-- DatabaseSize: 1499136
-- Created: 11-07-2010 10:46

-- User Table information:
-- Number of tables: 9
-- Categories: 8 row(s)
-- Customers: 91 row(s)
-- ELMAH: -1 row(s)
-- Employees: 15 row(s)
-- Order Details: 2820 row(s)
-- Orders: 1078 row(s)
-- Products: 77 row(s)
-- Shippers: 15 row(s)
-- Suppliers: 29 row(s)

SET IDENTITY_INSERT [Suppliers] ON;
GO
INSERT INTO [Suppliers] ([Supplier ID],[Company Name],[Contact Name],[Contact Title],[Address],[City],[Region],[Postal Code],[Country],[Phone],[Fax]) VALUES (1,N'Exotic Liquids',N'Charlotte Cooper',N'Purchasing Manager',N'49 Gilbert St.',N'London',null,N'EC1 4SD',N'UK',N'(71) 555-2222',null);
GO
";

            using (IRepository repo = new DB4Repository(chinookConnectionString))
            {
                string showPlan = string.Empty;
                var ds = repo.ExecuteSql(sql, out showPlan);
                Assert.IsTrue(ds.Tables.Count > 0);
                Assert.IsTrue(ds.Tables[0].Rows.Count > 0);
            }
          
        }

        [Test]
        public void TestServerDgml()
        {

            using (IRepository sourceRepository = new  ServerDBRepository4(serverConnectionString))
            {
                var generator = new Generator4(sourceRepository, @"C:\temp\test2.dgml");
                generator.GenerateSchemaGraph(serverConnectionString);
            }
        }

        [Test]
        public void TestGraphSort()
        {
            using (IRepository sourceRepository = new DB4Repository(sdfConnectionString))
            {
                var generator = new Generator4(sourceRepository, @"C:\temp\testAMS40.sqlce");
                generator.ExcludeTables(new System.Collections.Generic.List<String>());
            }
        }

        [Test]
        public void TestGraphSortServer()
        {
            using (IRepository sourceRepository = new ServerDBRepository4(serverConnectionString))
            {
                var generator = new Generator4(sourceRepository, @"C:\temp\testAMS40.sqlce");
                generator.ExcludeTables(new System.Collections.Generic.List<String>());
            }
        }

        [Test]
        public void TestCeDgml()
        {
            using (IRepository sourceRepository = new DB4Repository(chinookConnectionString))
            {
                var generator = new Generator4(sourceRepository, @"C:\temp\testChinook40.dgml");
                generator.GenerateSchemaGraph(chinookConnectionString);
            }
        }

        [Test]
        public void TestDiffNullRef()
        {
            string target = @"Data Source=C:\Data\SQLCE\Test\DiffNullRefDatabases\ArtistManager.sdf";
            string source = @"Data Source=C:\Data\SQLCE\Test\DiffNullRefDatabases\ArtistManagerDesignDatabase.sdf";

            using (IRepository sourceRepository = new DB4Repository(source))
            {
                var generator = new Generator4(sourceRepository);
                using (IRepository targetRepository = new DB4Repository(target))
                {
                    SqlCeDiff.CreateDiffScript(sourceRepository, targetRepository, generator, false);
                }
            }
        }
        
        
        //[Test]
        //public void TestImportBoolean()
        //{
        //    using (IRepository repository = new DB4Repository(@"Data Source=C:\Data\SQLCE\Test\Empty Site40.sdf"))
        //    {
        //        var generator = new Generator4(repository);
        //        using (var reader = new Kent.Boogaart.KBCsv.CsvReader(@"C:\Data\SQLCE\offmst\offmst.txt"))
        //        {
        //            reader.ValueSeparator = ',';
        //            Kent.Boogaart.KBCsv.HeaderRecord hr = reader.ReadHeaderRecord();
        //            if (generator.ValidColumns("offmst", hr.Values))
        //            {
        //                foreach (Kent.Boogaart.KBCsv.DataRecord record in reader.DataRecords)
        //                {
        //                    generator.GenerateTableInsert("offmst", hr.Values, record.Values);
        //                }
        //            }

        //        }
        //    }
        //}


        //C:\Data\SQLCE\ImportNullStringExample\BookNullString.csv

        //[Test]
        //public void TestImportEmptyString()
        //{
        //    using (IRepository repository = new DB4Repository(@"Data Source=C:\Data\SQLCE\Test\Empty Site40.sdf"))
        //    {
        //        var generator = new Generator4(repository);
        //        using (var reader = new Kent.Boogaart.KBCsv.CsvReader(@"C:\Data\SQLCE\ImportNullStringExample\BookNullString.csv"))
        //        {
        //            reader.ValueSeparator = ';';
        //            Kent.Boogaart.KBCsv.HeaderRecord hr = reader.ReadHeaderRecord();
        //            if (generator.ValidColumns("Book", hr.Values))
        //            {
        //                foreach (Kent.Boogaart.KBCsv.DataRecord record in reader.DataRecords)
        //                {
        //                    generator.GenerateTableInsert("Book", hr.Values, record.Values);
        //                }
        //            }

        //        }
        //    }
        //}


        //[Test]
        //public void TestImportValidColUpdate()
        //{
        //    using (IRepository repository = new DB4Repository(@"Data Source=C:\Data\SQLCE\Test\test.sdf"))
        //    {
        //        var generator = new Generator4(repository);
        //        using (var reader = new Kent.Boogaart.KBCsv.CsvReader(@"C:\Data\SQLCE\routes.csv"))
        //        {
        //            reader.ValueSeparator = ',';
        //            Kent.Boogaart.KBCsv.HeaderRecord hr = reader.ReadHeaderRecord();
        //            if (generator.ValidColumns("Routes", hr.Values))
        //            {
        //                foreach (Kent.Boogaart.KBCsv.DataRecord record in reader.DataRecords)
        //                {
        //                    generator.GenerateTableInsert("Routes", hr.Values, record.Values);
        //                }
        //            }

        //        }
        //        Assert.IsTrue(generator.GeneratedScript.Length == 808);
        //    }
        //}



    }

