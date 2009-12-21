using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;

namespace ExportSqlCE
{
    class ServerDBRepository : IRepository
    {
        private readonly string _connectionString;
        private SqlConnection cn;
        private delegate void AddToListDelegate<T>(ref List<T> list, SqlDataReader dr);

        public ServerDBRepository(string connectionString)
        {
            _connectionString = connectionString;
            cn = new SqlConnection(_connectionString);
            cn.Open();
        }

        public void Dispose()
        {
            if (cn != null)
            {
                cn.Close();
                cn = null;
            }
        }

        private static void AddToListString(ref List<string> list, SqlDataReader dr)
        {
            list.Add(dr.GetString(0));
        }

        private static void AddToListColumns(ref List<Column> list, SqlDataReader dr)
        {
            list.Add(new Column
            {
                ColumnName = dr.GetString(0)
                , IsNullable = (YesNoOption)Enum.Parse(typeof(YesNoOption), dr.GetString(1))
                , DataType = dr.GetString(2)
                , CharacterMaxLength = (dr.IsDBNull(3) ? 0 : dr.GetInt32(3))
                , NumericPrecision = (dr.IsDBNull(4) ? 0 : Convert.ToInt32(dr[4], System.Globalization.CultureInfo.InvariantCulture))
#if V35
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

        private static void AddToListConstraints(ref List<Constraint> list, SqlDataReader dr)
        {
            list.Add(new Constraint
            {
                ConstraintTableName = dr.GetString(0)
                , ConstraintName = dr.GetString(1)
                , ColumnName = string.Format(System.Globalization.CultureInfo.InvariantCulture, "[{0}]", dr.GetString(2))
                , UniqueConstraintTableName = dr.GetString(3)
                , UniqueConstraintName = dr.GetString(4)
                , UniqueColumnName = string.Format(System.Globalization.CultureInfo.InvariantCulture, "[{0}]", dr.GetString(5))
                , UpdateRule = dr.GetString(6)
                , DeleteRule  = dr.GetString(7)
            });
        }

        private void AddToListIndexes(ref List<Index> list, SqlDataReader dr)
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

        private List<T> ExecuteReader<T>(string commandText, AddToListDelegate<T> AddToListMethod)
        {
            List<T> list = new List<T>();
            using (var cmd = new SqlCommand(commandText, cn))
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
            using (var cmd = new SqlCommand(commandText, cn))
            {
                return cmd.ExecuteReader();
            }
        }

        private DataTable ExecuteDataTable(string commandText)
        {
            DataTable dt = new DataTable();
            dt.Locale = System.Globalization.CultureInfo.InvariantCulture;
            using (var cmd = new SqlCommand(commandText, cn))
            {
                using (var da = new SqlDataAdapter(cmd))
                {
                    da.Fill(dt);
                }
            }
            return dt;
        }

        private object ExecuteScalar(string commandText)
        {
            object val;
            using (var cmd = new SqlCommand(commandText, cn))
            {
                val = cmd.ExecuteScalar();
            }
            return val;
        }

        private void ExecuteNonQuery(string commandText)
        {
            using (var cmd = new SqlCommand(commandText, cn))
            {
                cmd.ExecuteNonQuery();
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
            return -1;
        }

        public Int64 GetRowCount(string tableName)
        {
            return -1;
        }

        public bool HasIdentityColumn(string tableName)
        {
            return (ExecuteScalar("SELECT is_identity FROM sys.columns INNER JOIN sys.objects ON sys.columns.object_id = sys.objects.object_id WHERE sys.objects.name = '" + tableName + "' AND sys.objects.type = 'U' AND sys.columns.is_identity = 1") != null);
        }

        public List<string> GetAllTableNames()
        {
            return ExecuteReader(
                "SELECT table_name FROM information_schema.tables WHERE TABLE_TYPE = N'BASE TABLE' ORDER BY table_name"
                , new AddToListDelegate<string>(AddToListString));
        }

        public List<KeyValuePair<string, string>> GetDatabaseInfo()
        {
            return new List<KeyValuePair<string,string>>();
        }

        public List<Column> GetColumnsFromTable()
        {
//            SELECT DISTINCT 
//ORDINAL_POSITION,
//Column_name, 
//col.is_nullable, 
//data_type, 
//character_maximum_length, 
//numeric_precision, 
//autoinc_increment =
//      CASE cols.is_identity 
//         WHEN 0 THEN 0
//         WHEN 1 THEN IDENT_INCR(col.TABLE_NAME)
//      END,      
//autoinc_seed =
//      CASE cols.is_identity 
//         WHEN 0 THEN 0
//         WHEN 1 THEN IDENT_SEED(col.TABLE_NAME)
//      END,      
//column_hasdefault =
//      CASE 
//         WHEN col.COLUMN_DEFAULT IS NULL THEN 0
//         ELSE 1
//      END,      
//column_default, 
//column_flags =
//CASE cols.is_rowguidcol   
//     WHEN 0 THEN 0 
//     ELSE 378
//END,   
//numeric_scale, 
//table_name,
//autoinc_next =
//      CASE cols.is_identity 
//         WHEN 0 THEN 0
//         WHEN 1 THEN IDENT_CURRENT(col.TABLE_NAME)
//      END      

//         FROM         information_schema.columns col
//         JOIN sys.columns cols on col.COLUMN_NAME = cols.name 
//         AND cols.object_id = OBJECT_ID(col.table_name)
//         WHERE      SUBSTRING(COLUMN_NAME, 1,5) <> '__sys'
//         ORDER BY ordinal_position ASC
         



            return ExecuteReader(
                "SELECT     Column_name, is_nullable, data_type, character_maximum_length, numeric_precision, autoinc_increment, autoinc_seed, column_hasdefault, column_default, column_flags, numeric_scale, table_name, autoinc_next  " +
                "FROM         information_schema.columns " +
                "WHERE      SUBSTRING(COLUMN_NAME, 1,5) <> '__sys'  " +
                "ORDER BY ordinal_position ASC "
                , new AddToListDelegate<Column>(AddToListColumns));
        }

        public IDataReader GetDataFromReader(string tableName)
        {
            return ExecuteDataReader(string.Format("SELECT * FROM [{0}]",tableName));
        }

        public DataTable GetDataFromTable(string tableName)
        {
            return ExecuteDataTable(string.Format(System.Globalization.CultureInfo.InvariantCulture, "Select * From [{0}]", tableName));
        }
        
        public List<string> GetPrimaryKeysFromTable(string tableName)
        {
            return ExecuteReader(
                "SELECT u.COLUMN_NAME " +
                "FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS c INNER JOIN " +
                    "INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS u ON c.CONSTRAINT_NAME = u.CONSTRAINT_NAME " +
                "where u.TABLE_NAME = '" + tableName + "' AND c.TABLE_NAME = '" + tableName + "' and c.CONSTRAINT_TYPE = 'PRIMARY KEY'"
                , new AddToListDelegate<string>(AddToListString));
        }
        
        public List<Constraint> GetAllForeignKeys()
        {

//            SELECT f.name AS ForeignKey,
//OBJECT_NAME(f.parent_object_id) AS TableName,
//COL_NAME(fc.parent_object_id,
//fc.parent_column_id) AS ColumnName,
//OBJECT_NAME (f.referenced_object_id) AS ReferenceTableName,
//COL_NAME(fc.referenced_object_id,
//fc.referenced_column_id) AS ReferenceColumnName
//FROM sys.foreign_keys AS f
//INNER JOIN sys.foreign_key_columns AS fc
//ON f.OBJECT_ID = fc.constraint_object_id

            
            return ExecuteReader(
                "SELECT KCU1.TABLE_NAME AS FK_TABLE_NAME,  KCU1.CONSTRAINT_NAME AS FK_CONSTRAINT_NAME, KCU1.COLUMN_NAME AS FK_COLUMN_NAME, " +
                "KCU2.TABLE_NAME AS UQ_TABLE_NAME, KCU2.CONSTRAINT_NAME AS UQ_CONSTRAINT_NAME, KCU2.COLUMN_NAME AS UQ_COLUMN_NAME, RC.UPDATE_RULE, RC.DELETE_RULE, KCU2.ORDINAL_POSITION AS UQ_ORDINAL_POSITION, KCU1.ORDINAL_POSITION AS FK_ORDINAL_POSITION " +
                "FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS RC " +
                "JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE KCU1 ON KCU1.CONSTRAINT_NAME = RC.CONSTRAINT_NAME " +
                "JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE KCU2 ON  KCU2.CONSTRAINT_NAME =  RC.UNIQUE_CONSTRAINT_NAME AND KCU2.ORDINAL_POSITION = KCU1.ORDINAL_POSITION AND KCU2.TABLE_NAME = RC.UNIQUE_CONSTRAINT_TABLE_NAME " +
                "ORDER BY FK_TABLE_NAME, FK_CONSTRAINT_NAME, FK_ORDINAL_POSITION"
                , new AddToListDelegate<Constraint>(AddToListConstraints));
        }

        public List<Constraint> GetAllForeignKeys(string tableName)
        {
            return ExecuteReader(
                "SELECT KCU1.TABLE_NAME AS FK_TABLE_NAME,  KCU1.CONSTRAINT_NAME AS FK_CONSTRAINT_NAME, KCU1.COLUMN_NAME AS FK_COLUMN_NAME, " +
                "KCU2.TABLE_NAME AS UQ_TABLE_NAME, KCU2.CONSTRAINT_NAME AS UQ_CONSTRAINT_NAME, KCU2.COLUMN_NAME AS UQ_COLUMN_NAME, RC.UPDATE_RULE, RC.DELETE_RULE, KCU2.ORDINAL_POSITION AS UQ_ORDINAL_POSITION, KCU1.ORDINAL_POSITION AS FK_ORDINAL_POSITION " +
                "FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS RC " +
                "JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE KCU1 ON KCU1.CONSTRAINT_NAME = RC.CONSTRAINT_NAME " +
                "JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE KCU2 ON  KCU2.CONSTRAINT_NAME =  RC.UNIQUE_CONSTRAINT_NAME AND KCU2.ORDINAL_POSITION = KCU1.ORDINAL_POSITION AND KCU2.TABLE_NAME = RC.UNIQUE_CONSTRAINT_TABLE_NAME " +
                "WHERE KCU1.TABLE_NAME = '" + tableName + "' " + 
                "ORDER BY FK_TABLE_NAME, FK_CONSTRAINT_NAME, FK_ORDINAL_POSITION"
                , new AddToListDelegate<Constraint>(AddToListConstraints));
        }


        /// <summary>
        /// Get the query based on http://msdn.microsoft.com/en-us/library/ms174156.aspx
        /// </summary>
        /// <returns></returns>
        
        public List<Index> GetIndexesFromTable(string tableName)
        {
//            select top 4096	
//    index_name = i.name,
//    i.is_unique_constraint,
//    0,
//    ic.key_ordinal,
//    column_name = c.name,
//    ic.is_descending_key 
//from 
//    sys.indexes i
//left outer join
//    sys.index_columns ic on i.object_id = ic.object_id and i.index_id = ic.index_id
//left outer join
//    sys.columns c on c.object_id = ic.object_id and c.column_id = ic.column_id
//where 
//    i.object_id = object_id('FilesToIndexTbl')
//    AND i.name IS NOT NULL
//order by
//    i.name,
//    case key_ordinal 
//            when 0 then 256 
//                else ic.key_ordinal 
//    end


            return ExecuteReader(
                "SELECT     TABLE_NAME, INDEX_NAME, PRIMARY_KEY, [UNIQUE], [CLUSTERED], ORDINAL_POSITION, COLUMN_NAME, COLLATION AS SORT_ORDER " + // Weird column name COLLATION FOR SORT_ORDER
                "FROM         Information_Schema.Indexes "+
                "WHERE     (PRIMARY_KEY = 0) " +
                "   AND (TABLE_NAME = '" + tableName + "')  " +
                "   AND (SUBSTRING(COLUMN_NAME, 1,5) <> '__sys')   " +
                "ORDER BY TABLE_NAME, INDEX_NAME, ORDINAL_POSITION"
                , new AddToListDelegate<Index>(AddToListIndexes));
        }

        public void RenameTable(string oldName, string newName)
        {
            ExecuteNonQuery(string.Format("sp_rename '{0}', '{1}';", oldName, newName));            
        }

        public bool IsServer()
        {
            return true;
        }

        #endregion

    }
}
