﻿using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlServerCe;
using System.IO;
using System.Text;
using System.Globalization;

namespace ErikEJ.SqlCeScripting
{
    /// <summary>
    /// Implementation of the <see cref="IRepository"/> interface for SQL Server Compact 3.1/3.5
    /// </summary>
#if V40
    public class DB4Repository : IRepository
#else
    public class DBRepository : IRepository
#endif
    {
        private readonly string _connectionString;
        private SqlCeConnection cn;
        private delegate void AddToListDelegate<T>(ref List<T> list, SqlCeDataReader dr);
        private string showPlan = string.Empty;
        private bool schemaHasChanged = false;

        /// <summary>
        /// Initializes a new instance of the <see cref="DBRepository"/> class.
        /// </summary>
        /// <param name="connectionString">The connection string.</param>
#if V40
        public DB4Repository(string connectionString)
#else
        public DBRepository(string connectionString)
#endif
        {
            _connectionString = connectionString;
            cn = new SqlCeConnection(_connectionString);
            cn.Open();
        }

        /// <summary>
        /// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
        /// </summary>
        public void Dispose()
        {
            if (cn != null)
            {
                cn.Close();
                cn = null;
            }
        }

        private static void AddToListString(ref List<string> list, SqlCeDataReader dr)
        {
            list.Add(dr.GetString(0));
        }

        private static void AddToListColumns(ref List<Column> list, SqlCeDataReader dr)
        {
            list.Add(new Column
            {
                ColumnName = dr.GetString(0)
                , IsNullable = (YesNoOption)Enum.Parse(typeof(YesNoOption), dr.GetString(1))
                , DataType = dr.GetString(2)
                , CharacterMaxLength = (dr.IsDBNull(3) ? 0 : dr.GetInt32(3))
                , NumericPrecision = (dr.IsDBNull(4) ? 0 : Convert.ToInt32(dr[4], System.Globalization.CultureInfo.InvariantCulture))
#if V31
#else
                , AutoIncrementBy = (dr.IsDBNull(5) ? 0 : Convert.ToInt64(dr[5], System.Globalization.CultureInfo.InvariantCulture))
                , AutoIncrementSeed = (dr.IsDBNull(6) ? 0 : Convert.ToInt64(dr[6], System.Globalization.CultureInfo.InvariantCulture))
                , AutoIncrementNext = (dr.IsDBNull(12) ? 0 : Convert.ToInt64(dr[12], System.Globalization.CultureInfo.InvariantCulture))
#endif
                , ColumnHasDefault = (dr.IsDBNull(7) ? false : dr.GetBoolean(7))
                , ColumnDefault = (dr.IsDBNull(8) ? string.Empty : dr.GetString(8).Trim())
                , RowGuidCol = (dr.IsDBNull(9) ? false : dr.GetInt32(9) == 378 || dr.GetInt32(9) == 282)
                , NumericScale = (dr.IsDBNull(10) ? 0 : Convert.ToInt32(dr[10], System.Globalization.CultureInfo.InvariantCulture))
                , TableName = dr.GetString(11)
            });
        }

        private static void AddToListConstraints(ref List<Constraint> list, SqlCeDataReader dr)
        {
            list.Add(new Constraint
            {
                ConstraintTableName = dr.GetString(0)
                , ConstraintName = dr.GetString(1)
                , ColumnName = dr.GetString(2)
                , UniqueConstraintTableName = dr.GetString(3)
                , UniqueConstraintName = dr.GetString(4)
                , UniqueColumnName = dr.GetString(5)
                , UpdateRule = dr.GetString(6)
                , DeleteRule  = dr.GetString(7)
                , Columns = new ColumnList()
                , UniqueColumns = new ColumnList()
            });
        }

        private void AddToListIndexes(ref List<Index> list, SqlCeDataReader dr)
        {
            list.Add(new Index
            {
                TableName = dr.GetString(0)
                , IndexName = dr.GetString(1)
                , Unique = dr.GetBoolean(3)
                , Clustered = dr.GetBoolean(4)
                , OrdinalPosition = dr.GetInt32(5)
                , ColumnName = dr.GetString(6)                
                , SortOrder = (dr.GetInt16(7) == 1 ? SortOrderEnum.ASC : SortOrderEnum.DESC) 
            });

        }

        private void AddToListPrimaryKeys(ref List<PrimaryKey> list, SqlCeDataReader dr)
        {
            list.Add(new PrimaryKey 
            {
                ColumnName = dr.GetString(0)
                , KeyName = dr.GetString(1)
                , TableName = dr.GetString(2)
            });
        }

        private List<T> ExecuteReader<T>(string commandText, AddToListDelegate<T> AddToListMethod)
        {
            List<T> list = new List<T>();
            using (var cmd = new SqlCeCommand(commandText, cn))
            {
                using (var dr = cmd.ExecuteReader())
                {
                    while (dr.Read())
                        AddToListMethod(ref list, dr);
                }
            }
            return list;
        }

        private IDataReader ExecuteDataReader(string commandText)
        {
            using (var cmd = new SqlCeCommand(commandText, cn))
            {
                cmd.CommandType = CommandType.TableDirect;
                return cmd.ExecuteReader();
            }
        }

        private DataTable ExecuteDataTable(string commandText)
        {
            DataTable dt = new DataTable();
            dt.Locale = System.Globalization.CultureInfo.InvariantCulture;
            using (var cmd = new SqlCeCommand(commandText, cn))
            {
                using (var da = new SqlCeDataAdapter(cmd))
                {
                    da.Fill(dt);
                }
            }
            return dt;
        }

        private object ExecuteScalar(string commandText)
        {
            object val;
            using (var cmd = new SqlCeCommand(commandText, cn))
            {
                val = cmd.ExecuteScalar();
            }
            return val;
        }

        private void ExecuteNonQuery(string commandText)
        {
            using (var cmd = new SqlCeCommand(commandText, cn))
            {
                cmd.ExecuteNonQuery();
            }
        }


        private List<KeyValuePair<string, string>> GetSqlCeInfo()
        {
            List<KeyValuePair<string, string>> valueList = new List<KeyValuePair<string, string>>();
#if V31
#else
            valueList = cn.GetDatabaseInfo();
#endif
            valueList.Add(new KeyValuePair<string,string>("Database", cn.Database));
            valueList.Add(new KeyValuePair<string, string>("ServerVersion", cn.ServerVersion));
            if (System.IO.File.Exists(cn.Database))
            { 
                System.IO.FileInfo fi = new System.IO.FileInfo(cn.Database);
                valueList.Add(new KeyValuePair<string, string>("DatabaseSize", fi.Length.ToString(System.Globalization.CultureInfo.InvariantCulture)));
                valueList.Add(new KeyValuePair<string, string>("Created", fi.CreationTime.ToShortDateString() + " " + fi.CreationTime.ToShortTimeString()));
            }
            return valueList;
        }

        #region IRepository Members

        /// <summary>
        /// Gets the row version ordinal.
        /// </summary>
        /// <param name="tableName">Name of the table.</param>
        /// <returns></returns>
        public Int32 GetRowVersionOrdinal(string tableName)
        {
            object value = ExecuteScalar("SELECT ordinal_position FROM information_schema.columns WHERE TABLE_NAME = '" + tableName + "' AND data_type = 'rowversion'");
            if (value != null)
            {
                return (int)value - 1;
            }
            return -1;
        }

        /// <summary>
        /// Gets the row count.
        /// </summary>
        /// <param name="tableName">Name of the table.</param>
        /// <returns></returns>
        public Int64 GetRowCount(string tableName)
        {
            object value = ExecuteScalar("SELECT CARDINALITY FROM INFORMATION_SCHEMA.INDEXES WHERE PRIMARY_KEY = 1 AND TABLE_NAME = N'" + tableName + "'");
            if (value != null)
            {
                return (Int64)value;
            }
            return -1;
        }

        /// <summary>
        /// Determines whether [has identity column] [the specified table name].
        /// </summary>
        /// <param name="tableName">Name of the table.</param>
        /// <returns>
        /// 	<c>true</c> if [has identity column] [the specified table name]; otherwise, <c>false</c>.
        /// </returns>
        public bool HasIdentityColumn(string tableName)
        {
            return (ExecuteScalar("SELECT COLUMN_NAME FROM information_schema.columns WHERE TABLE_NAME = N'" + tableName + "' AND AUTOINC_SEED IS NOT NULL") != null);
        }

        /// <summary>
        /// Gets all table names.
        /// </summary>
        /// <returns></returns>
        public List<string> GetAllTableNames()
        {
            return ExecuteReader(
                "SELECT table_name FROM information_schema.tables WHERE TABLE_TYPE = N'TABLE'"
                , new AddToListDelegate<string>(AddToListString));
        }

        public List<string> GetAllTableNamesForExclusion()
        {
            return GetAllTableNames();
        }

        public List<string> GetAllSubscriptionNames()
        {
            object value = ExecuteScalar("SELECT table_name FROM information_schema.tables WHERE TABLE_NAME = '__sysMergeSubscriptions' ");
            if (value == null)
            {
                return new List<string>();
            }
            else
            {
                return ExecuteReader(
                    "SELECT Publisher + ':' + PublisherDatabase + ':' + Publication as Sub FROM __sysMergeSubscriptions ORDER BY Publisher, PublisherDatabase, Publication"
                    , new AddToListDelegate<string>(AddToListString));
            }
        }


//        SELECT Publisher + ':' + PublisherDatabase + ':' + Publication as Sub FROM __sysMergeSubscriptions
//ORDER BY Publisher + ':' + PublisherDatabase + ':' + Publication 


        /// <summary>
        /// Gets the database info.
        /// </summary>
        /// <returns></returns>
        public List<KeyValuePair<string, string>> GetDatabaseInfo()
        {
            return GetSqlCeInfo();
        }

        /// <summary>
        /// Gets the columns from table.
        /// </summary>
        /// <returns></returns>
        public List<Column> GetColumnsFromTable()
        {
            return ExecuteReader(
                "SELECT     Column_name, is_nullable, data_type, character_maximum_length, numeric_precision, autoinc_increment, autoinc_seed, column_hasdefault, column_default, column_flags, numeric_scale, table_name, autoinc_next  " +
                "FROM         information_schema.columns " +
                "WHERE      SUBSTRING(COLUMN_NAME, 1,5) <> '__sys'  " +
                "ORDER BY ordinal_position ASC "
                , new AddToListDelegate<Column>(AddToListColumns));
        }

        /// <summary>
        /// Gets the data from reader.
        /// </summary>
        /// <param name="tableName">Name of the table.</param>
        /// <returns></returns>
        public IDataReader GetDataFromReader(string tableName)
        {
            return ExecuteDataReader(tableName);
        }

        /// <summary>
        /// Gets the data from table.
        /// </summary>
        /// <param name="tableName">Name of the table.</param>
        /// <param name="columns">The columns.</param>
        /// <returns></returns>
        public DataTable GetDataFromTable(string tableName, List<Column> columns)
        {
            // Include the schema name, may not always be dbo!
            System.Text.StringBuilder sb = new System.Text.StringBuilder(200);
            foreach (Column col in columns)
            {
                sb.Append(string.Format(System.Globalization.CultureInfo.InvariantCulture, "[{0}], ", col.ColumnName));
            }
            sb.Remove(sb.Length - 2, 2);
            return ExecuteDataTable(string.Format(System.Globalization.CultureInfo.InvariantCulture, "Select {0} From [{1}]", sb.ToString(), tableName));
        }

        /// <summary>
        /// Gets the primary keys from table.
        /// </summary>
        /// <param name="tableName">Name of the table.</param>
        /// <returns></returns>
        public List<PrimaryKey> GetAllPrimaryKeys()
        {
            var list = ExecuteReader(
                "SELECT u.COLUMN_NAME, c.CONSTRAINT_NAME, c.TABLE_NAME " +
                "FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS c INNER JOIN " +
                "INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS u ON c.CONSTRAINT_NAME = u.CONSTRAINT_NAME AND u.TABLE_NAME = c.TABLE_NAME " +
                "where c.CONSTRAINT_TYPE = 'PRIMARY KEY' ORDER BY u.TABLE_NAME, c.CONSTRAINT_NAME, u.ORDINAL_POSITION"
                , new AddToListDelegate<PrimaryKey>(AddToListPrimaryKeys));

            return Helper.EnsureUniqueNames(list);
        }

        /// <summary>
        /// Gets all foreign keys.
        /// </summary>
        /// <returns></returns>
        public List<Constraint> GetAllForeignKeys()
        {
            var list = ExecuteReader(
                "SELECT DISTINCT KCU1.TABLE_NAME AS FK_TABLE_NAME,  KCU1.CONSTRAINT_NAME AS FK_CONSTRAINT_NAME, KCU1.COLUMN_NAME AS FK_COLUMN_NAME, " +
                "KCU2.TABLE_NAME AS UQ_TABLE_NAME, KCU2.CONSTRAINT_NAME AS UQ_CONSTRAINT_NAME, KCU2.COLUMN_NAME AS UQ_COLUMN_NAME, RC.UPDATE_RULE, RC.DELETE_RULE, KCU2.ORDINAL_POSITION AS UQ_ORDINAL_POSITION, KCU1.ORDINAL_POSITION AS FK_ORDINAL_POSITION " +
                "FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS RC " +
                "JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE KCU1 ON KCU1.CONSTRAINT_NAME = RC.CONSTRAINT_NAME " +
                "JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE KCU2 ON  KCU2.CONSTRAINT_NAME =  RC.UNIQUE_CONSTRAINT_NAME AND KCU2.ORDINAL_POSITION = KCU1.ORDINAL_POSITION AND KCU2.TABLE_NAME = RC.UNIQUE_CONSTRAINT_TABLE_NAME " +
                "ORDER BY FK_TABLE_NAME, FK_CONSTRAINT_NAME, FK_ORDINAL_POSITION"
                , new AddToListDelegate<Constraint>(AddToListConstraints));

            return Helper.GetGroupForeingKeys(list, GetAllTableNames());
        }

        /// <summary>
        /// Gets all foreign keys.
        /// </summary>
        /// <param name="tableName">Name of the table.</param>
        /// <returns></returns>
        public List<Constraint> GetAllForeignKeys(string tableName)
        {
            var list = ExecuteReader(
                "SELECT KCU1.TABLE_NAME AS FK_TABLE_NAME,  KCU1.CONSTRAINT_NAME AS FK_CONSTRAINT_NAME, KCU1.COLUMN_NAME AS FK_COLUMN_NAME, " +
                "KCU2.TABLE_NAME AS UQ_TABLE_NAME, KCU2.CONSTRAINT_NAME AS UQ_CONSTRAINT_NAME, KCU2.COLUMN_NAME AS UQ_COLUMN_NAME, RC.UPDATE_RULE, RC.DELETE_RULE, KCU2.ORDINAL_POSITION AS UQ_ORDINAL_POSITION, KCU1.ORDINAL_POSITION AS FK_ORDINAL_POSITION " +
                "FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS RC " +
                "JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE KCU1 ON KCU1.CONSTRAINT_NAME = RC.CONSTRAINT_NAME " +
                "JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE KCU2 ON  KCU2.CONSTRAINT_NAME =  RC.UNIQUE_CONSTRAINT_NAME AND KCU2.ORDINAL_POSITION = KCU1.ORDINAL_POSITION AND KCU2.TABLE_NAME = RC.UNIQUE_CONSTRAINT_TABLE_NAME " +
                "WHERE KCU1.TABLE_NAME = '" + tableName + "' " +
                "ORDER BY FK_TABLE_NAME, FK_CONSTRAINT_NAME, FK_ORDINAL_POSITION"
                , new AddToListDelegate<Constraint>(AddToListConstraints));
            return Helper.GetGroupForeingKeys(list, GetAllTableNames());
        }

        /// <summary>
        /// Get the query based on http://msdn.microsoft.com/en-us/library/ms174156.aspx
        /// </summary>
        /// <param name="tableName">Name of the table.</param>
        /// <returns></returns>
        public List<Index> GetIndexesFromTable(string tableName)
        {
            return ExecuteReader(
                "SELECT     TABLE_NAME, INDEX_NAME, PRIMARY_KEY, [UNIQUE], [CLUSTERED], ORDINAL_POSITION, COLUMN_NAME, COLLATION AS SORT_ORDER " + // Weird column name COLLATION FOR SORT_ORDER
                "FROM         Information_Schema.Indexes "+
                "WHERE     (PRIMARY_KEY = 0) " +
                "   AND (TABLE_NAME = '" + tableName + "')  " +
                "   AND (SUBSTRING(COLUMN_NAME, 1,5) <> '__sys')   " +
                "ORDER BY TABLE_NAME, INDEX_NAME, ORDINAL_POSITION"
                , new AddToListDelegate<Index>(AddToListIndexes));
        }

        /// <summary>
        /// Renames the table.
        /// </summary>
        /// <param name="oldName">The old name.</param>
        /// <param name="newName">The new name.</param>
        public void RenameTable(string oldName, string newName)
        {
            ExecuteNonQuery(string.Format(System.Globalization.CultureInfo.InvariantCulture, "sp_rename '{0}', '{1}';", oldName, newName));            
        }

        /// <summary>
        /// Determines whether this instance is server.
        /// </summary>
        /// <returns>
        /// 	<c>true</c> if this instance is server; otherwise, <c>false</c>.
        /// </returns>
        public bool IsServer()
        {
            return false;
        }

        /// <summary>
        /// Executes the SQL.
        /// </summary>
        /// <param name="script">The script.</param>
        /// <returns></returns>
        public DataSet ExecuteSql(string script)
        {
            DataSet ds = new DataSet();
            RunCommands(ds, script, false, false);
            return ds;
        }

        public DataSet ExecuteSql(string script, out bool schemaChanged)
        {
            schemaHasChanged = false;
            DataSet ds = new DataSet();
            RunCommands(ds, script, false, false);
            schemaChanged = schemaHasChanged;
            return ds;
        }

        public string ParseSql(string script)
        {
            DataSet ds = new DataSet();
            RunCommands(ds, script, true, false);
            return showPlan;
        }

        public DataSet ExecuteSql(string script, out string showPlanString)
        {
            DataSet ds = new DataSet();
            RunCommands(ds, script, false, true);
            showPlanString = showPlan;
            return ds;
        }

        public DataSet ExecuteSql(string script, out string showPlanString, out bool schemaChanged)
        {
            schemaHasChanged = false;
            DataSet ds = new DataSet();
            RunCommands(ds, script, false, true);
            showPlanString = showPlan;
            schemaChanged = schemaHasChanged;
            return ds;
        }

        internal void RunCommands(DataSet dataset, string script, bool checkSyntax, bool includePlan)
        {
            dataset.EnforceConstraints = false;
            using (SqlCeCommand cmd = new SqlCeCommand())
            {
                cmd.Connection = cn;
                if (checkSyntax)
                {
                    cmd.CommandText = "SET SHOWPLAN_XML ON";
                    cmd.ExecuteNonQuery();
                }

                if (includePlan)
                {
                    cmd.CommandText = "SET STATISTICS XML ON;";
                    cmd.ExecuteNonQuery();
                }

                StringBuilder sb = new StringBuilder(10000);
                using (StringReader reader = new StringReader(script))
                {
                    string line;
                    while ((line = reader.ReadLine()) != null)
                    {
                        line = line.Trim();
                        if (line.Equals("GO", StringComparison.OrdinalIgnoreCase))
                        {
                            RunCommand(sb.ToString(), dataset);
                            sb.Remove(0, sb.Length);
                        }
                        else
                        {
                            sb.Append(line);
                            sb.Append(Environment.NewLine);
                        }
                    }
                }
                if (checkSyntax)
                {
                    cmd.CommandText = "SELECT @@SHOWPLAN;";
                    
                    object obj = cmd.ExecuteScalar();
                    if (obj.GetType() == typeof(System.String))
                        showPlan = (string)obj;

                    cmd.CommandText = "SET SHOWPLAN_XML OFF";
                    cmd.ExecuteNonQuery();
                }

                if (includePlan)
                {
                    cmd.CommandText = "SELECT @@SHOWPLAN;";

                    object obj = cmd.ExecuteScalar();
                    if (obj.GetType() == typeof(System.String))
                        showPlan = (string)obj;

                    cmd.CommandText = "SET STATISTICS XML OFF";
                    cmd.ExecuteNonQuery();
                }


            }
        }

        internal void RunCommand(string commandText, DataSet dataSet)
        {
            using (SqlCeCommand cmd = new SqlCeCommand())
            {
                cmd.CommandText = commandText;
                cmd.Connection = cn;

                CommandExecute execute = FindExecuteType(commandText);

                if (execute != CommandExecute.Undefined)
                {
                    if (execute == CommandExecute.DataTable)
                    {
                        string[] tables = new string[1];
                        tables[0] = "table" + dataSet.Tables.Count.ToString();
                        SqlCeDataReader rdr = cmd.ExecuteReader();
                        dataSet.Load(cmd.ExecuteReader(), LoadOption.OverwriteChanges, tables);
                        dataSet.Tables[dataSet.Tables.Count - 1].MinimumCapacity = 0;
                        dataSet.Tables[dataSet.Tables.Count - 1].Locale = CultureInfo.InvariantCulture;
                    }
                    if (execute == CommandExecute.NonQuery || execute == CommandExecute.NonQuerySchemaChanged)
                    {
                        if (execute == CommandExecute.NonQuerySchemaChanged)
                            schemaHasChanged = true;
                        int rows = cmd.ExecuteNonQuery();
                        DataTable table = new DataTable();
                        table.MinimumCapacity = Math.Max(0, rows);
                        dataSet.Tables.Add(table);
                    }
                }
            }
        }

        private enum CommandExecute
        {
            Undefined,
            DataTable,
            NonQuery,
            NonQuerySchemaChanged
        }

        private static CommandExecute FindExecuteType(string commandText)
        {
            if (string.IsNullOrEmpty(commandText.Trim()))
            {
                return CommandExecute.Undefined;
            }
            string test = commandText.Trim();

            if (test.ToUpperInvariant().StartsWith("SELECT ", StringComparison.Ordinal))
            {
                return CommandExecute.DataTable;
            }
            else if (test.ToLowerInvariant().StartsWith("sp_", StringComparison.Ordinal))
            {
                return CommandExecute.DataTable;
            }
            else if (test.ToUpperInvariant().StartsWith("CREATE ") || test.ToUpperInvariant().StartsWith("ALTER ") || test.ToUpperInvariant().StartsWith("DROP "))
            {
                return CommandExecute.NonQuerySchemaChanged;
            }
            else
            {
                return CommandExecute.NonQuery;
            }
        }

        /// <summary>
        /// Get the local Datetime for last sync
        /// </summary>
        /// <param name="publication"> Publication id: EEJx:Northwind:NwPubl</param>
        /// <returns></returns>
        public DateTime GetLastSuccessfulSyncTime(string publication)
        {
            SqlCeCommand cmd = null;

            string[] vals = publication.Split(':');

            if (vals.Length != 3)
                return DateTime.MinValue;

            using (cmd = cn.CreateCommand())
            {
                cmd.Connection = cn;

                cmd.CommandText = "SELECT table_name FROM information_schema.tables WHERE TABLE_NAME = @table";
                cmd.Parameters.Add("@table", SqlDbType.NVarChar, 4000);
                cmd.Parameters["@table"].Value = "__sysMergeSubscriptions";
                object obj = cmd.ExecuteScalar();

                if (obj == null)
                    return DateTime.MinValue;
                cmd.Parameters.Clear();

                cmd.CommandText = "SELECT LastSuccessfulSync FROM __sysMergeSubscriptions " +
                    "WHERE Publisher=@publisher AND PublisherDatabase=@database AND Publication=@publication";
                
                cmd.Parameters.Add("@publisher", SqlDbType.NVarChar, 4000);
                cmd.Parameters["@publisher"].Value = vals[0];

                cmd.Parameters.Add("@database", SqlDbType.NVarChar, 4000);
                cmd.Parameters["@database"].Value = vals[1];                

                cmd.Parameters.Add("@publication", SqlDbType.NVarChar, 4000);
                cmd.Parameters["@publication"].Value = vals[2];

                obj = cmd.ExecuteScalar();
                if (obj == null)
                    return DateTime.MinValue;
                else
                    return ((DateTime)obj);
            }
        }


        #endregion

    }
}
