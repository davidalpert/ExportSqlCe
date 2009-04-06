using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlServerCe;
using System.Linq;
using System.Text;

namespace ExportSQLCE
{
    class DBRepository : IRepository
    {
        private string _connectionString;
        public DBRepository(string connectionString)
        {
            _connectionString = connectionString;
        }

        private delegate void AddToListDelegate<T>(ref List<T> list, SqlCeDataReader dr);
        private void AddToListString(ref List<string> list, SqlCeDataReader dr)
        {
            list.Add(dr.GetString(0));
        }
        private void AddToListColumns(ref List<Column> list, SqlCeDataReader dr)
        {
            list.Add(new Column()
            {
                ColumnName = dr.GetString(0)
                , IsNullable = (YesNoOptionEnum)Enum.Parse(typeof(YesNoOptionEnum), dr.GetString(1))
                , DataType = dr.GetString(2)
                , CharacterMaxLength = (dr.IsDBNull(3) ? 0 : dr.GetInt32(3))
                , NumericPrecision = (dr.IsDBNull(4) ? 0 : Convert.ToInt32(dr[4]))
#if V35
                , AutoIncrementBy = (dr.IsDBNull(5) ? 0 : Convert.ToInt64(dr[5]))
                , AutoIncrementSeed = (dr.IsDBNull(6) ? 0 : Convert.ToInt64(dr[6]))
#endif
                , ColumnHasDefault = (dr.IsDBNull(7) ? false : dr.GetBoolean(7))
                , ColumnDefault = (dr.IsDBNull(8) ? string.Empty : dr.GetString(8).Trim())
                , RowGuidCol = (dr.IsDBNull(9) ? false : dr.GetInt32(9) == 378)
                , NumericScale = (dr.IsDBNull(10) ? 0 : Convert.ToInt32(dr[10]))
            });
        }
        private void AddToListConstraints(ref List<Constraint> list, SqlCeDataReader dr)
        {
            list.Add(new Constraint()
            {
                ConstraintTableName = dr.GetString(0)
                , ConstraintName = dr.GetString(1)
                , ColumnName = string.Format("[{0}]", dr.GetString(2))
                , UniqueConstraintTableName = dr.GetString(3)
                , UniqueConstraintName = dr.GetString(4)
                , UniqueColumnName = string.Format("[{0}]", dr.GetString(5))
                , UpdateRule = dr.GetString(6)
                , DeleteRule  = dr.GetString(7)
            });
        }
        private void AddToListIndexes(ref List<Index> list, SqlCeDataReader dr)
        {
            list.Add(new Index()
            {
                TableName = dr.GetString(0)
                , IndexName = dr.GetString(1)
                , PrimaryKey = dr.GetBoolean(2)
                , Unique = dr.GetBoolean(3)
                , Clustered = dr.GetBoolean(4)
                , OrdinalPosition = dr.GetInt32(5)
                , ColumnName = dr.GetString(6)                
                , SortOrder = (dr.GetInt16(7) == 1 ? SortOrderEnum.Asc : SortOrderEnum.Desc) 
            });

        }

        private List<T> ExecuteReader<T>(string commandText, AddToListDelegate<T> AddToListMethod)
        {
            List<T> list = new List<T>();
            using (SqlCeConnection cn = new SqlCeConnection(_connectionString))
            {
                cn.Open();
                using (SqlCeCommand cmd = new SqlCeCommand(
                    commandText, cn))
                {
                    using (SqlCeDataReader dr = cmd.ExecuteReader())
                    {
                        while (dr.Read())
                            AddToListMethod(ref list, dr);

                    }
                }
            }
            return list;
        }

        private DataTable ExecuteDataTable(string commandText)
        {
            DataTable dt = new DataTable();
            using (SqlCeConnection cn = new SqlCeConnection(_connectionString))
            {
                cn.Open();
                using (SqlCeCommand cmd = new SqlCeCommand(
                    commandText, cn))
                {
                    using (SqlCeDataAdapter da = new SqlCeDataAdapter(cmd))
                    {
                        da.Fill(dt);
                    }
                }
            }
            return dt;
        }

        private object ExecuteScalar(string commandText)
        {
            object val = null;
            using (SqlCeConnection cn = new SqlCeConnection(_connectionString))
            {
                cn.Open();
                using (SqlCeCommand cmd = new SqlCeCommand(
                    commandText, cn))
                {
                    val = cmd.ExecuteScalar();
                }
            }
            return val;
        }

        private List<KeyValuePair<string, string>> GetSqlCeInfo()
        {
            using (SqlCeConnection cn = new SqlCeConnection(_connectionString))
            {
                cn.Open();
                List<KeyValuePair<string, string>> valueList = new List<KeyValuePair<string, string>>();
#if V35
                valueList = cn.GetDatabaseInfo();
#endif
                valueList.Add(new KeyValuePair<string,string>("Database", cn.Database));
                valueList.Add(new KeyValuePair<string, string>("ServerVersion", cn.ServerVersion));
                if (System.IO.File.Exists(cn.Database))
                { 
                    System.IO.FileInfo fi = new System.IO.FileInfo(cn.Database);
                    valueList.Add(new KeyValuePair<string,string>("DatabaseSize", fi.Length.ToString()));
                    valueList.Add(new KeyValuePair<string, string>("Created", fi.CreationTime.ToShortDateString() + " " + fi.CreationTime.ToShortTimeString()));
                }
                return valueList;
            }
        }

        #region IRepository Members

        public Int32 GetRowVersionOrdinal(string tableName)
        {
            object value = ExecuteScalar("SELECT ordinal_position FROM information_schema.columns WHERE TABLE_NAME = '" + tableName + "' AND data_type = 'rowversion'");
            if (value != null)
            {
                return (int)value - 1;
            }
            else
            {
                return -1;
            }
        }

        public Int64 GetRowCount(string tableName)
        {
            object value = ExecuteScalar("SELECT CARDINALITY FROM INFORMATION_SCHEMA.INDEXES WHERE PRIMARY_KEY = 1 AND TABLE_NAME = N'" + tableName + "'");
            if (value != null)
            {
                return (Int64)value;
            }
            else
            {
                return -1;
            }
        }

        public bool HasIdentityColumn(string tableName)
        {
            return (ExecuteScalar("SELECT COLUMN_NAME FROM information_schema.columns WHERE TABLE_NAME = N'" + tableName + "' AND AUTOINC_SEED IS NOT NULL") != null);
        }

        public List<string> GetAllTableNames()
        {
            return ExecuteReader<string>(
                "SELECT table_name FROM information_schema.tables WHERE TABLE_TYPE = N'TABLE'"
                , new AddToListDelegate<string>(AddToListString));
        }

        public List<KeyValuePair<string, string>> GetDatabaseInfo()
        {
            return GetSqlCeInfo();
        }

        public List<Column> GetColumnsFromTable(string tableName)
        {
            return ExecuteReader<Column>(
                "SELECT     Column_name, is_nullable, data_type, character_maximum_length, numeric_precision, autoinc_increment, autoinc_seed, column_hasdefault, column_default, column_flags, numeric_scale " +
                "FROM         information_schema.columns " +
                "WHERE     (table_name = '" + tableName + "') " +
                "AND     (column_name NOT LIKE '__sys%') " +
                "ORDER BY ordinal_position ASC "
                , new AddToListDelegate<Column>(AddToListColumns));
        }
        
        public DataTable GetDataFromTable(string tableName)
        {
            return ExecuteDataTable(string.Format("Select * From [{0}]", tableName));
        }
        
        public List<string> GetPrimaryKeysFromTable(string tableName)
        {
            return ExecuteReader<string>(
                "SELECT u.COLUMN_NAME " +
                "FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS c INNER JOIN " +
                    "INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS u ON c.CONSTRAINT_NAME = u.CONSTRAINT_NAME " +
                "where u.TABLE_NAME = '" + tableName + "' AND c.TABLE_NAME = '" + tableName + "' and c.CONSTRAINT_TYPE = 'PRIMARY KEY'"
                , new AddToListDelegate<string>(AddToListString));
        }
        
        public List<Constraint> GetAllForeignKeys()
        {
            return ExecuteReader<Constraint>(
                "SELECT KCU1.TABLE_NAME AS FK_TABLE_NAME,  KCU1.CONSTRAINT_NAME AS FK_CONSTRAINT_NAME, KCU1.COLUMN_NAME AS FK_COLUMN_NAME, " +
                "KCU2.TABLE_NAME AS UQ_TABLE_NAME, KCU2.CONSTRAINT_NAME AS UQ_CONSTRAINT_NAME, KCU2.COLUMN_NAME AS UQ_COLUMN_NAME, RC.UPDATE_RULE, RC.DELETE_RULE, KCU2.ORDINAL_POSITION AS UQ_ORDINAL_POSITION, KCU1.ORDINAL_POSITION AS FK_ORDINAL_POSITION " +
                "FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS RC " +
                "JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE KCU1 ON KCU1.CONSTRAINT_NAME = RC.CONSTRAINT_NAME " +
                "JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE KCU2 ON  KCU2.CONSTRAINT_NAME =  RC.UNIQUE_CONSTRAINT_NAME AND KCU2.ORDINAL_POSITION = KCU1.ORDINAL_POSITION AND KCU2.TABLE_NAME = RC.UNIQUE_CONSTRAINT_TABLE_NAME " +
                "ORDER BY FK_TABLE_NAME, FK_CONSTRAINT_NAME, FK_ORDINAL_POSITION"
                , new AddToListDelegate<Constraint>(AddToListConstraints));
        }

        /// <summary>
        /// Get the query based on http://msdn.microsoft.com/en-us/library/ms174156.aspx
        /// </summary>
        /// <returns></returns>
        
        public List<Index> GetIndexesFromTable(string tableName)
        {
            return ExecuteReader<Index>(
                "SELECT     TABLE_NAME, INDEX_NAME, PRIMARY_KEY, [UNIQUE], [CLUSTERED], ORDINAL_POSITION, COLUMN_NAME, COLLATION AS SORT_ORDER " + // Weird column name COLLATION FOR SORT_ORDER
                "FROM         Information_Schema.Indexes "+
                "WHERE     (PRIMARY_KEY = 0) " +
                "   AND (TABLE_NAME = '" + tableName + "')  " +
                "   AND (INDEX_NAME NOT LIKE '__sys%')  " +
                "ORDER BY TABLE_NAME, INDEX_NAME, ORDINAL_POSITION"
                , new AddToListDelegate<Index>(AddToListIndexes));
        }

        #endregion

    }
}
