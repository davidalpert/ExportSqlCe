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
        private const string serverConnectionString = @"data source=(local);User=amsdbuser;Password=amsdbuser;Initial Catalog=AMS;";

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
    }

