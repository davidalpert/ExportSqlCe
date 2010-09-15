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
    }
}
