using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlServerCe;
using System.IO;
using System.Text;
using System.Globalization;

namespace ErikEJ.SqlCeScripting
{
    public class DBRepository : IRepository
    {
        private readonly string _connectionString;
        private SqlCeConnection cn;
        private delegate void AddToListDelegate<T>(ref List<T> list, SqlCeDataReader dr);

        public DBRepository(string connectionString)
        {
            _connectionString = connectionString;
            cn = new SqlCeConnection(_connectionString);
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
#if V35
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
            object value = ExecuteScalar("SELECT CARDINALITY FROM INFORMATION_SCHEMA.INDEXES WHERE PRIMARY_KEY = 1 AND TABLE_NAME = N'" + tableName + "'");
            if (value != null)
            {
                return (Int64)value;
            }
            return -1;
        }

        public bool HasIdentityColumn(string tableName)
        {
            return (ExecuteScalar("SELECT COLUMN_NAME FROM information_schema.columns WHERE TABLE_NAME = N'" + tableName + "' AND AUTOINC_SEED IS NOT NULL") != null);
        }

        public List<string> GetAllTableNames()
        {
            return ExecuteReader(
                "SELECT table_name FROM information_schema.tables WHERE TABLE_TYPE = N'TABLE'"
                , new AddToListDelegate<string>(AddToListString));
        }

        public List<KeyValuePair<string, string>> GetDatabaseInfo()
        {
            return GetSqlCeInfo();
        }

        public List<Column> GetColumnsFromTable()
        {
            return ExecuteReader(
                "SELECT     Column_name, is_nullable, data_type, character_maximum_length, numeric_precision, autoinc_increment, autoinc_seed, column_hasdefault, column_default, column_flags, numeric_scale, table_name, autoinc_next  " +
                "FROM         information_schema.columns " +
                "WHERE      SUBSTRING(COLUMN_NAME, 1,5) <> '__sys'  " +
                "ORDER BY ordinal_position ASC "
                , new AddToListDelegate<Column>(AddToListColumns));
        }

        public IDataReader GetDataFromReader(string tableName)
        {
            return ExecuteDataReader(tableName);
        }

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
        
        public List<PrimaryKey> GetPrimaryKeysFromTable(string tableName)
        {
            return ExecuteReader(
                "SELECT u.COLUMN_NAME, c.CONSTRAINT_NAME " +
                "FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS c INNER JOIN " +
                    "INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS u ON c.CONSTRAINT_NAME = u.CONSTRAINT_NAME " +
                "where u.TABLE_NAME = '" + tableName + "' AND c.TABLE_NAME = '" + tableName + "' and c.CONSTRAINT_TYPE = 'PRIMARY KEY'"
                , new AddToListDelegate<PrimaryKey>(AddToListPrimaryKeys));
        }
        
        public List<Constraint> GetAllForeignKeys()
        {
            var list = ExecuteReader(
                "SELECT KCU1.TABLE_NAME AS FK_TABLE_NAME,  KCU1.CONSTRAINT_NAME AS FK_CONSTRAINT_NAME, KCU1.COLUMN_NAME AS FK_COLUMN_NAME, " +
                "KCU2.TABLE_NAME AS UQ_TABLE_NAME, KCU2.CONSTRAINT_NAME AS UQ_CONSTRAINT_NAME, KCU2.COLUMN_NAME AS UQ_COLUMN_NAME, RC.UPDATE_RULE, RC.DELETE_RULE, KCU2.ORDINAL_POSITION AS UQ_ORDINAL_POSITION, KCU1.ORDINAL_POSITION AS FK_ORDINAL_POSITION " +
                "FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS RC " +
                "JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE KCU1 ON KCU1.CONSTRAINT_NAME = RC.CONSTRAINT_NAME " +
                "JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE KCU2 ON  KCU2.CONSTRAINT_NAME =  RC.UNIQUE_CONSTRAINT_NAME AND KCU2.ORDINAL_POSITION = KCU1.ORDINAL_POSITION AND KCU2.TABLE_NAME = RC.UNIQUE_CONSTRAINT_TABLE_NAME " +
                "ORDER BY FK_TABLE_NAME, FK_CONSTRAINT_NAME, FK_ORDINAL_POSITION"
                , new AddToListDelegate<Constraint>(AddToListConstraints));
            return Helper.GetGroupForeingKeys(list);
        }

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
            return Helper.GetGroupForeingKeys(list);
        }

        /// <summary>
        /// Get the query based on http://msdn.microsoft.com/en-us/library/ms174156.aspx
        /// </summary>
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

        public void RenameTable(string oldName, string newName)
        {
            ExecuteNonQuery(string.Format(System.Globalization.CultureInfo.InvariantCulture, "sp_rename '{0}', '{1}';", oldName, newName));            
        }

        public bool IsServer()
        {
            return false;
        }

        public DataSet ExecuteSql(string script)
        {
            DataSet ds = new DataSet();
            RunCommands(ds, script);
            return ds;
        }

        internal void RunCommands(DataSet dataset, string script)
        {
            StringBuilder sb = new StringBuilder(10000);
            using (StringReader reader = new StringReader(script))
            {
                string line;
                while ((line = reader.ReadLine()) != null)
                {
                    if (line.Equals("GO", StringComparison.OrdinalIgnoreCase))
                    {
                        RunCommand(sb.ToString(), dataset);
                        sb.Remove(0, sb.Length);
                    }
                    else
                    {
                        if (!line.StartsWith("-- "))
                        {
                            sb.Append(line);
                            sb.Append(Environment.NewLine);
                        }
                    }
                }
            }
        }

        internal void RunCommand(string commandText, DataSet dataSet)
        {
            using (SqlCeCommand cmd = new SqlCeCommand())
            {
                cmd.CommandText = commandText;

                CommandExecute execute = FindExecuteType(commandText);

                if (execute != CommandExecute.Undefined)
                {
                    if (execute == CommandExecute.DataTable)
                    {
                        string result = RunDataReader(cmd, cn, Convert.ToChar(" ", CultureInfo.InvariantCulture), false);
                        dataSet.Tables.Add(GetDataTable(result, "Result"));
                    }
                    if (execute == CommandExecute.NonQuery)
                    {
                        int rows = RunNonQuery(cmd);
                        dataSet.Tables.Add(GetDataTable(rows.ToString(CultureInfo.InvariantCulture), "Rows affected"));
                    }
                }
            }
        }

        private string RunDataReader(SqlCeCommand cmd, SqlCeConnection connection, Char colSepChar, bool removeSpaces)
        {
            bool writeHeaders = true;
            int headerInterval = Int32.MaxValue;
            StringBuilder sbres = new StringBuilder(100000);

            cmd.Connection = connection;
            SqlCeDataReader rdr = cmd.ExecuteReader();
            int rows = 0;
            int maxWidth = 256;
            string colSep = colSepChar.ToString();
            List<Column> headings = new List<Column>();
            CreateHeadings(cmd, cn, rdr, maxWidth, headings);
            while (rdr.Read())
            {
                bool doWrite = (rows == 0 && writeHeaders);
                if (!doWrite && rows > 0)
                    doWrite = ((rows % headerInterval) == 0);

                if (doWrite)
                {
                    for (int x = 0; x < rdr.FieldCount; x++)
                    {
                        if (removeSpaces)
                        {
                            sbres.Append(headings[x].ColumnName);
                        }
                        else
                        {
                            sbres.Append(headings[x].ColumnName.PadRight(headings[x].Width));
                        }
                        sbres.Append(colSep);
                    }
                    Console.WriteLine();
                    for (int x = 0; x < rdr.FieldCount; x++)
                    {
                        System.Text.StringBuilder sb = new System.Text.StringBuilder();
                        if (removeSpaces)
                        {
                            sb.Append('-', headings[x].ColumnName.Length);
                        }
                        else
                        {
                            sb.Append('-', headings[x].Width);
                        }
                        sbres.Append(sb.ToString());
                        sbres.Append(colSep);
                    }
                    sbres.Append(Environment.NewLine);
                }
                for (int i = 0; i < rdr.FieldCount; i++)
                {
                    if (!rdr.IsDBNull(i))
                    {
                        string value = string.Empty;
                        string fieldType = rdr.GetDataTypeName(i);
                        if (fieldType == "Image" || fieldType == "VarBinary" || fieldType == "Binary" || fieldType == "RowVersion")
                        {
                            Byte[] buffer = (Byte[])rdr[i];
                            StringBuilder sb = new StringBuilder();
                            sb.Append("0x");
                            for (int y = 0; y < (headings[i].Width - 2) / 2; y++)
                            {
                                sb.Append(buffer[y].ToString("X2", CultureInfo.InvariantCulture));
                            }
                            value = sb.ToString();
                        }
                        else
                        {
                            value = Convert.ToString(rdr[i], CultureInfo.InvariantCulture);
                        }

                        if (removeSpaces)
                        {
                            sbres.Append(value);
                        }
                        else if (headings[i].PadLeft)
                        {
                            sbres.Append(value.PadLeft(headings[i].Width));
                        }
                        else
                        {
                            sbres.Append(value.PadRight(headings[i].Width));
                        }
                    }
                    else
                    {
                        if (removeSpaces)
                        {
                            sbres.Append("NULL");
                        }
                        else
                        {
                            sbres.Append("NULL".PadRight(headings[i].Width));
                        }
                    }
                    sbres.Append(colSep);
                }
                rows++;
                sbres.Append(Environment.NewLine);
            }
            return sbres.ToString();
        }

        private int RunNonQuery(SqlCeCommand cmd)
        {
            cmd.Connection = cn;
            return cmd.ExecuteNonQuery();
        }

        private DataTable GetDataTable(string value, string fieldName)
        {
            DataTable dataTable = new DataTable();
            DataColumn dataColumn_ID = new DataColumn(fieldName, typeof(String));
            dataTable.Columns.Add(dataColumn_ID);
            DataRow dataRow;
            dataRow = dataTable.NewRow();
            dataRow[fieldName] = value; 
            dataTable.Rows.Add(dataRow);
            dataTable.AcceptChanges();
            return dataTable;
        }

        private enum CommandExecute
        {
            Undefined,
            DataTable,
            NonQuery
        }

        private static void CreateHeadings(SqlCeCommand cmd, SqlCeConnection conn, SqlCeDataReader rdr, int maxWidth, List<Column> headings)
        {
            for (int i = 0; i < rdr.FieldCount; i++)
            {
                // 18 different types
                // Calculate width as max of name or data type based width
                switch (rdr.GetDataTypeName(i))
                {
                    case "BigInt":
                        int width = Math.Max(20, rdr.GetName(i).Length);
                        headings.Add(new Column { ColumnName = rdr.GetName(i), Width = width, PadLeft = true });
                        break;

                    case "Binary":
                        width = Math.Max(GetFieldSize(conn, rdr.GetName(i), maxWidth, cmd.CommandText), rdr.GetName(i).Length) + 2;
                        headings.Add(new Column { ColumnName = rdr.GetName(i), Width = width, PadLeft = false });
                        break;

                    case "Bit":
                        width = Math.Max(5, rdr.GetName(i).Length);
                        headings.Add(new Column { ColumnName = rdr.GetName(i), Width = width, PadLeft = true });
                        break;

                    case "DateTime":
                        width = Math.Max(20, rdr.GetName(i).Length);
                        headings.Add(new Column { ColumnName = rdr.GetName(i), Width = width, PadLeft = false });
                        break;

                    case "Float":
                        width = Math.Max(24, rdr.GetName(i).Length);
                        headings.Add(new Column { ColumnName = rdr.GetName(i), Width = width, PadLeft = true });
                        break;

                    case "Image":
                        width = Math.Max(GetFieldSize(conn, rdr.GetName(i), maxWidth, cmd.CommandText), rdr.GetName(i).Length) + 2;
                        headings.Add(new Column { ColumnName = rdr.GetName(i), Width = width, PadLeft = false });
                        break;

                    case "Int":
                        width = Math.Max(11, rdr.GetName(i).Length);
                        headings.Add(new Column { ColumnName = rdr.GetName(i), Width = width, PadLeft = true });
                        break;

                    case "Money":
                        width = Math.Max(21, rdr.GetName(i).Length);
                        headings.Add(new Column { ColumnName = rdr.GetName(i), Width = width, PadLeft = true });
                        break;

                    case "NChar":
                        width = Math.Max(GetFieldSize(conn, rdr.GetName(i), maxWidth, cmd.CommandText), rdr.GetName(i).Length);
                        headings.Add(new Column { ColumnName = rdr.GetName(i), Width = width, PadLeft = false });
                        break;

                    case "NText":
                        width = Math.Max(GetFieldSize(conn, rdr.GetName(i), maxWidth, cmd.CommandText), rdr.GetName(i).Length);
                        headings.Add(new Column { ColumnName = rdr.GetName(i), Width = width, PadLeft = false });
                        break;

                    case "Numeric":
                        width = Math.Max(21, rdr.GetName(i).Length);
                        headings.Add(new Column { ColumnName = rdr.GetName(i), Width = width, PadLeft = true });
                        break;

                    case "NVarChar":
                        width = Math.Max(GetFieldSize(conn, rdr.GetName(i), maxWidth, cmd.CommandText), rdr.GetName(i).Length);
                        headings.Add(new Column { ColumnName = rdr.GetName(i), Width = width, PadLeft = false });
                        break;

                    case "Real":
                        width = Math.Max(14, rdr.GetName(i).Length);
                        headings.Add(new Column { ColumnName = rdr.GetName(i), Width = width, PadLeft = true });
                        break;

                    case "RowVersion":
                        width = Math.Max(8, rdr.GetName(i).Length) + 2;
                        headings.Add(new Column { ColumnName = rdr.GetName(i), Width = width, PadLeft = false });
                        break;

                    case "SmallInt":
                        width = Math.Max(6, rdr.GetName(i).Length);
                        headings.Add(new Column { ColumnName = rdr.GetName(i), Width = width, PadLeft = true });
                        break;

                    case "TinyInt":
                        width = Math.Max(3, rdr.GetName(i).Length);
                        headings.Add(new Column { ColumnName = rdr.GetName(i), Width = width, PadLeft = true });
                        break;

                    case "UniqueIdentifier":
                        width = Math.Max(36, rdr.GetName(i).Length);
                        headings.Add(new Column { ColumnName = rdr.GetName(i), Width = width, PadLeft = false });
                        break;

                    case "VarBinary":
                        width = Math.Max(GetFieldSize(conn, rdr.GetName(i), maxWidth, cmd.CommandText), rdr.GetName(i).Length) + 2;
                        headings.Add(new Column { ColumnName = rdr.GetName(i), Width = width, PadLeft = false });
                        break;

                    default:
                        break;
                }
            }
        }

        private static int GetFieldSize(SqlCeConnection conn, string fieldName, int maxWidth, string commandText)
        {
            using (SqlCeCommand cmdSize = new SqlCeCommand(commandText))
            {
                cmdSize.Connection = conn;
                using (SqlCeDataReader rdr = cmdSize.ExecuteReader(System.Data.CommandBehavior.SchemaOnly | System.Data.CommandBehavior.KeyInfo))
                {
                    System.Data.DataTable schemaTable = rdr.GetSchemaTable();
                    System.Data.DataView schemaView = new System.Data.DataView(schemaTable);
                    schemaView.RowFilter = string.Format(CultureInfo.InvariantCulture, "ColumnName = '{0}'", fieldName);
                    if (schemaView.Count > 0)
                    {
                        string colName = schemaView[0].Row["BaseColumnName"].ToString();
                        string tabName = schemaView[0].Row["BaseTableName"].ToString();
                        using (SqlCeCommand cmd = new SqlCeCommand(string.Format(CultureInfo.InvariantCulture, "SELECT CHARACTER_MAXIMUM_LENGTH FROM INFORMATION_SCHEMA.COLUMNS WHERE COLUMN_NAME = '{0}' AND TABLE_NAME = '{1}'", colName, tabName)))
                        {
                            cmd.Connection = conn;
                            Object val = cmd.ExecuteScalar();
                            if (val != null)
                            {
                                if ((int)val < maxWidth)
                                {
                                    return (int)val;
                                }
                                else
                                {
                                    return maxWidth;
                                }
                            }
                        }
                    }
                }
            }

            return -1;
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
            else
            {
                return CommandExecute.NonQuery;
            }
        }

        #endregion
    }
}
