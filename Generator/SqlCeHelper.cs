using System;
using System.Data.SqlServerCe;
using System.Data.SqlClient;

namespace ErikEJ.SqlCeScripting
{
#if V40 
    public class SqlCeHelper4 : ISqlCeHelper 
#else
    public class SqlCeHelper : ISqlCeHelper 
#endif
    {
        public string FormatError(Exception ex)
        {
            if (ex.GetType() == typeof(SqlCeException))
            {
                return Helper.ShowErrors((SqlCeException)ex);
            }
            else if (ex.GetType() == typeof(SqlException))
            {
                return Helper.ShowErrors((SqlException)ex);
            }
            else return ex.ToString();
        }

        public string GetFullConnectionString(string connectionString)
        {
            using (SqlCeReplication repl = new SqlCeReplication())
            {
                repl.SubscriberConnectionString = connectionString;
                return repl.SubscriberConnectionString;
            }
        }

        public void CreateDatabase(string connectionString)
        {
            using (SqlCeEngine engine = new SqlCeEngine(connectionString))
            {
                engine.CreateDatabase();
            }
        }
#if V40
        public void UpgradeTo40(string connectionString)
        {
            string filename;
            using (SqlCeConnection conn = new SqlCeConnection(connectionString))
            {
                filename = conn.Database;
            }
            if (filename.Contains("|DataDirectory|"))
                throw new ApplicationException("DataDirectory macro not supported for upgrade");

            SQLCEVersion fileversion = DetermineVersion(filename);
            if (fileversion == SQLCEVersion.SQLCE20)
                throw new ApplicationException("Unable to upgrade from 2.0 to 4.0");

            if (SQLCEVersion.SQLCE40 > fileversion)
            {
                SqlCeEngine engine = new SqlCeEngine(connectionString);
                engine.Upgrade();
            }
        }


        public string PathFromConnectionString(string connectionString)
        {
            SqlCeConnectionStringBuilder sb = new SqlCeConnectionStringBuilder(connectionString);
            return sb.DataSource;
        }

#else
        public void UpgradeTo40(string connectionString)
        {
            throw new NotImplementedException("Not implemented");
        }

        public string PathFromConnectionString(string connectionString)
        { 
            throw new NotImplementedException("Not implemented");
        }
#endif

        public SQLCEVersion DetermineVersion(string fileName)
        {
            var versionDictionary = new System.Collections.Generic.Dictionary<int, SQLCEVersion> 
        { 
            { 0x73616261, SQLCEVersion.SQLCE20 }, 
            { 0x002dd714, SQLCEVersion.SQLCE30},
            { 0x00357b9d, SQLCEVersion.SQLCE35},
            { 0x003d0900, SQLCEVersion.SQLCE40}
        };
            int versionLONGWORD = 0;
            try
            {
                using (var fs = new System.IO.FileStream(fileName, System.IO.FileMode.Open))
                {
                    fs.Seek(16, System.IO.SeekOrigin.Begin);
                    using (System.IO.BinaryReader reader = new System.IO.BinaryReader(fs))
                    {
                        versionLONGWORD = reader.ReadInt32();
                    }
                }
            }
            catch
            {
                throw;
            }
            if (versionDictionary.ContainsKey(versionLONGWORD))
            {
                return versionDictionary[versionLONGWORD];
            }
            else
            {
                throw new ApplicationException("Unable to determine database file version");
            }
        }

        public bool IsV40Installed()
        {
            try
            {
                System.Reflection.Assembly.Load("System.Data.SqlServerCe, Version=4.0.0.0, Culture=neutral, PublicKeyToken=89845dcd8080cc91");
            }
            catch (System.IO.FileNotFoundException)
            {
                return false;
            }
            try
            {
                var factory = System.Data.Common.DbProviderFactories.GetFactory("System.Data.SqlServerCe.4.0");
            }
            catch (System.Configuration.ConfigurationException)
            {
                return false;
            }
            catch (System.ArgumentException)
            {
                return false;
            }
            return true;
        }

        public bool IsV35Installed()
        {
            try
            {
                System.Reflection.Assembly.Load("System.Data.SqlServerCe, Version=3.5.1.0, Culture=neutral, PublicKeyToken=89845dcd8080cc91");
            }
            catch (System.IO.FileNotFoundException)
            {
                return false;
            }
            try
            {
                var factory = System.Data.Common.DbProviderFactories.GetFactory("System.Data.SqlServerCe.3.5");
            }
            catch (System.Configuration.ConfigurationException)
            {
                return false;
            }
            catch (System.ArgumentException)
            {
                return false;
            }
            return true;
        }

    }
}
