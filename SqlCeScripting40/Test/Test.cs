    using System;
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
        private const string serverConnectionString = @"data source=(local);Initial Catalog=xavier;Integrated Security=true";

        [Test]
        public void ExerciseEngineWithTable()
        {
            using (IRepository sourceRepository = new DB4Repository(sdfConnectionString))
            {
                var generator = new Generator4(sourceRepository);
                using (IRepository targetRepository = new ServerDBRepository4(serverConnectionString))
                {
                    SqlCeDiff.CreateDiffScript(sourceRepository, targetRepository, generator);
                }

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




    }

