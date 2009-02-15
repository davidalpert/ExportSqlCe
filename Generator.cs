using System;
using System.Collections.Generic;
using System.Text;
using System.Linq;
using System.Data;

namespace ExportSQLCE
{
    class Generator
    {
        private IRepository _repository;
        private String _outFile;
        private StringBuilder _sbScript;
        private List<string> _tableNames;
        private Int32 _fileCounter = -1;

        private string _sep = "GO" + System.Environment.NewLine; 
        
        public Generator(IRepository repository, string outFile)
        {
            _outFile = outFile;
            _repository = repository;
            _sbScript = new StringBuilder(10000);

            _sbScript.AppendFormat("-- {0} {1}", DateTime.Now.ToShortDateString(), DateTime.Now.ToShortTimeString());
            _sbScript.AppendLine();

            _sbScript.Append("-- Database information:");
            _sbScript.AppendLine();

            List<KeyValuePair<string, string>> dbinfo = _repository.GetDatabaseInfo();
            foreach (KeyValuePair<string, string> kv in _repository.GetDatabaseInfo())
            {
                _sbScript.Append("-- ");
                _sbScript.Append(kv.Key);
                _sbScript.Append(": ");
                _sbScript.Append(kv.Value);
                _sbScript.AppendLine();
            }
            _sbScript.AppendLine();

            // Populate all tablenames
            _tableNames = _repository.GetAllTableNames();
            _sbScript.Append("-- User Table information:");
            _sbScript.AppendLine();
            _sbScript.Append("-- ");
            _sbScript.Append("Number of tables: ");
            _sbScript.Append(_tableNames.Count);
            _sbScript.AppendLine();
            
            foreach (string tableName in _tableNames)
            {
                Int64 rowCount = _repository.GetRowCount(tableName);
                _sbScript.Append("-- ");
                _sbScript.Append(tableName);
                _sbScript.Append(": ");
                _sbScript.Append(rowCount);
                _sbScript.Append(" row(s)");
                _sbScript.AppendLine();
            }
            _sbScript.AppendLine();
        }

        public string GenerateTables()
        {
            Console.WriteLine("Generating the tables....");
            _tableNames.ForEach(delegate(string tableName)
            {
                List<Column> columns = _repository.GetColumnsFromTable(tableName);                

                if (columns.Count > 0)
                {
                    _sbScript.AppendFormat("CREATE TABLE [{0}] (", tableName);

                    columns.ForEach(delegate(Column col)
                    {
                        switch (col.DataType)
                        {
                            case "nvarchar":
                                _sbScript.AppendFormat("[{0}] {1}({2}) {3} {4} {5}, "
                                    , col.ColumnName
                                    , col.DataType
                                    , col.CharacterMaxLength
                                    , (col.IsNullable == YesNoOptionEnum.YES ? "NULL" : "NOT NULL")
                                    , (col.ColumnHasDefault ? "DEFAULT " + col.ColumnDefault : string.Empty)
                                    , System.Environment.NewLine);
                                break;
                            case "nchar":
                                _sbScript.AppendFormat("[{0}] {1}({2}) {3} {4} {5}, "
                                    , col.ColumnName
                                    , "nchar"
                                    , col.CharacterMaxLength
                                    , (col.IsNullable == YesNoOptionEnum.YES ? "NULL" : "NOT NULL")
                                    , (col.ColumnHasDefault ? "DEFAULT " + col.ColumnDefault : string.Empty)
                                    , System.Environment.NewLine);
                                break;
                            case "numeric":
                                _sbScript.AppendFormat("[{0}] {1}({2},{3}) {4} {5} {6}, "
                                    , col.ColumnName
                                    , col.DataType
                                    , col.NumericPrecision
                                    , col.NumericScale
                                    , (col.IsNullable == YesNoOptionEnum.YES ? "NULL" : "NOT NULL")
                                    , (col.ColumnHasDefault ? "DEFAULT " + col.ColumnDefault : string.Empty)
                                    , System.Environment.NewLine);
                                break;
                            case "binary":
                                _sbScript.AppendFormat("[{0}] {1}({2}) {3} {4} {5}, "
                                    , col.ColumnName
                                    , col.DataType
                                    , col.CharacterMaxLength
                                    , (col.IsNullable == YesNoOptionEnum.YES ? "NULL" : "NOT NULL")
                                    , (col.ColumnHasDefault ? "DEFAULT " + col.ColumnDefault : string.Empty)
                                    , System.Environment.NewLine);
                                break;
                            case "varbinary":
                                _sbScript.AppendFormat("[{0}] {1}({2}) {3} {4} {5}, "
                                    , col.ColumnName
                                    , col.DataType
                                    , col.CharacterMaxLength                                    , (col.IsNullable == YesNoOptionEnum.YES ? "NULL" : "NOT NULL")
                                    , (col.ColumnHasDefault ? "DEFAULT " + col.ColumnDefault : string.Empty)
                                    , System.Environment.NewLine);
                                break;
                            default:
                                _sbScript.AppendFormat("[{0}] {1} {2} {3} {4}{5} {6}, "
                                    , col.ColumnName
                                    , col.DataType
                                    , (col.IsNullable == YesNoOptionEnum.YES ? "NULL" : "NOT NULL")
                                    , (col.ColumnHasDefault ? "DEFAULT " + col.ColumnDefault : string.Empty)
                                    , (col.RowGuidCol ? "ROWGUIDCOL" : string.Empty)
                                    , (col.AutoIncrementBy > 0 ? string.Format("IDENTITY ({0},{1})", col.AutoIncrementSeed, col.AutoIncrementBy) : string.Empty)
                                    , System.Environment.NewLine);
                                break;
                        }   
                    });

                    // Remove the last comma
                    _sbScript.Remove(_sbScript.Length - 2, 2);
                    _sbScript.AppendFormat(");{0}", System.Environment.NewLine);
                    _sbScript.Append(_sep);
                }
            });

            return _sbScript.ToString();
        }
        public string GenerateTableContent()
        {
            Console.WriteLine("Generating the data....");
            _tableNames.ForEach(delegate(string tableName)
            {
                // Skip rowversion column
                Int32 rowVersionOrdinal = _repository.GetRowVersionOrdinal(tableName);
                DataTable dt = _repository.GetDataFromTable(tableName);
                bool hasIdentity = _repository.HasIdentityColumn(tableName);
#if V35
                if (hasIdentity)
                {
                    _sbScript.Append(string.Format("SET IDENTITY_INSERT [{0}] ON", tableName));
                    _sbScript.Append(System.Environment.NewLine);
                    _sbScript.Append(_sep);
                }
#endif         
                string scriptPrefix = GetInsertScriptPrefix(tableName, dt);
                
                for (int iRow = 0; iRow < dt.Rows.Count; iRow++)
                {
                    _sbScript.Append(scriptPrefix);
                    for (int iColumn = 0; iColumn < dt.Columns.Count; iColumn++)
                    {
                        //Skip rowversion column
                        if (rowVersionOrdinal == iColumn || dt.Columns[iColumn].ColumnName.StartsWith("__sys"))
                        {
                            continue;
                        }
                        if (dt.Rows[iRow][iColumn] == System.DBNull.Value)
                        {
                            _sbScript.Append("null");
                        }
                        else if (dt.Columns[iColumn].DataType == typeof(System.String))
                        {
                            _sbScript.AppendFormat("N'{0}'", dt.Rows[iRow][iColumn].ToString().Replace("'", "''"));
                        }
                        else if (dt.Columns[iColumn].DataType == typeof(System.DateTime))
                        {
                            DateTime date = (DateTime)dt.Rows[iRow][iColumn];
                            //Datetime globalization - ODBC escape: {ts '2004-03-29 19:21:00'}
                            _sbScript.Append("{ts '");
                            _sbScript.Append(date.ToString("yyyy-MM-dd hh:mm:ss"));
                            _sbScript.Append("'}");
                        }
                        else if (dt.Columns[iColumn].DataType == typeof(System.Byte[]))
                        {
                            Byte[] buffer = (Byte[])dt.Rows[iRow][iColumn];
                            _sbScript.Append("0x");
                            for (int i = 0; i < buffer.Length; i++)
                            {
                                _sbScript.Append(buffer[i].ToString("X2"));
                            }
                        }
                        else if (dt.Columns[iColumn].DataType == typeof(System.Byte) || dt.Columns[iColumn].DataType == typeof(System.Int16) || dt.Columns[iColumn].DataType == typeof(System.Int32) || dt.Columns[iColumn].DataType == typeof(System.Int64) || dt.Columns[iColumn].DataType == typeof(System.Double) || dt.Columns[iColumn].DataType == typeof(System.Single) || dt.Columns[iColumn].DataType == typeof(System.Decimal))
                        {
                            string intString = Convert.ToString(dt.Rows[iRow][iColumn], System.Globalization.CultureInfo.InvariantCulture);
                            _sbScript.Append(intString);
                        }
                        else if (dt.Columns[iColumn].DataType == typeof(System.Boolean))
                        {
                            bool boolVal = (Boolean)dt.Rows[iRow][iColumn];
                            if (boolVal) 
                            { _sbScript.Append("1"); } 
                            else
                            { _sbScript.Append("0"); }
                        }
                        else
                        {
                            //Decimal point globalization
                            string value = Convert.ToString(dt.Rows[iRow][iColumn], System.Globalization.CultureInfo.InvariantCulture);
                            _sbScript.AppendFormat("'{0}'", value.Replace("'", "''"));
                        }
                        if (iColumn != (rowVersionOrdinal - 1))
                        {
                            if (dt.Columns.Count > iColumn + 1 && !dt.Columns[iColumn+1].ColumnName.StartsWith("__sys"))
                            { 
                                _sbScript.Append(iColumn != dt.Columns.Count - 1 ? "," : "");    
                            }
                            
                        }
                    }
                    _sbScript.Append(");");
                    _sbScript.Append(System.Environment.NewLine);
                    _sbScript.Append(_sep);
                }
#if V35
                if (hasIdentity)
                {
                    _sbScript.Append(string.Format("SET IDENTITY_INSERT [{0}] OFF", tableName));
                    _sbScript.Append(System.Environment.NewLine);
                    _sbScript.Append(_sep);
                }
#endif
                if (_sbScript.Length > 10485760)
                {
                    _fileCounter++;
                    Helper.WriteIntoFile(_sbScript.ToString(), _outFile, _fileCounter);
                    _sbScript.Remove(0, _sbScript.Length);
                }
            });

            return _sbScript.ToString();
        }        
        public string GeneratePrimaryKeys()
        {
            Console.WriteLine("Generating the primary keys....");
            _tableNames.ForEach(delegate(string tableName)
            {
                List<string> primaryKeys = _repository.GetPrimaryKeysFromTable(tableName);

                if (primaryKeys.Count > 0)
                {
                    _sbScript.AppendFormat("ALTER TABLE [{0}] ADD PRIMARY KEY (", tableName);

                    primaryKeys.ForEach(delegate(string columnName)
                    {
                        _sbScript.AppendFormat("[{0}]", columnName);
                        _sbScript.Append(",");
                    });

                    // Remove the last comma
                    _sbScript.Remove(_sbScript.Length - 1, 1);
                    _sbScript.AppendFormat(");{0}", System.Environment.NewLine);
                    _sbScript.Append(_sep);
                }
            });

            return _sbScript.ToString();
        }

        public string GenerateForeignKeys()
        {
            Console.WriteLine("Generating the foreign keys....");

            List<Constraint> foreignKeys = _repository.GetAllForeignKeys();
            //List<Constraint> foreignKeys = _repository.GetAllGroupedForeignKeys();
            List<Constraint> foreingKeysGrouped = this.GetGroupForeingKeys(foreignKeys);

            foreingKeysGrouped.ForEach(delegate(Constraint constraint)
            {
                _sbScript.AppendFormat("ALTER TABLE [{0}] ADD CONSTRAINT [{1}] FOREIGN KEY ({2}) REFERENCES [{3}]({4}) ON DELETE {5} ON UPDATE {6};{7}"
                    , constraint.ConstraintTableName
                    , constraint.ConstraintName
                    , constraint.ColumnName
                    , constraint.UniqueConstraintTableName
                    , constraint.UniqueColumnName
                    , constraint.DeleteRule
                    , constraint.UpdateRule
                    , System.Environment.NewLine);
                _sbScript.Append(_sep);
            });            
            return _sbScript.ToString();
        }

        /// <summary>
        /// Added at 18 September 2008, based on Todd Fulmino's comment on 28 August 2008, gosh it's almost a month man :P
        /// </summary>
        /// <returns></returns>
        public string GenerateIndex()
        {
            Console.WriteLine("Generating the indexes....");
            _tableNames.ForEach(delegate(string tableName)
            {
                List<Index> tableIndexes = _repository.GetIndexesFromTable(tableName);

                if (tableIndexes.Count > 0)
                {
                    IEnumerable<string> uniqueIndexNameList = tableIndexes.Select(i => i.IndexName).Distinct();

                    foreach (string uniqueIndexName in uniqueIndexNameList)
                    {
                        var indexesByName = from i in tableIndexes
                                                    where i.IndexName == uniqueIndexName
                                                    orderby i.OrdinalPosition
                                                    select i;

                        _sbScript.Append("CREATE ");

                        // Just get the first one to decide whether it's unique and/or clustered index
                        Index idx = indexesByName.First<Index>();
                        if (idx.Unique)
                            _sbScript.Append("UNIQUE ");
                        if (idx.Clustered)
                            _sbScript.Append("CLUSTERED ");

                        _sbScript.AppendFormat("INDEX [{0}] ON [{1}] (", idx.IndexName, idx.TableName);

                        foreach (Index col in indexesByName)
                        {
                            _sbScript.AppendFormat("[{0}] {1},", col.ColumnName, col.SortOrder.ToString());
                        }

                        // Remove the last comma
                        _sbScript.Remove(_sbScript.Length - 1, 1);
                        _sbScript.AppendLine(");");
                        _sbScript.Append(_sep);
                    }                    
                }
            });

            return _sbScript.ToString();            
        }

        public string GeneratedScript
        {
            get { return _sbScript.ToString(); }
        }

        public int FileCounter
        {

            get 
            {
                if (_fileCounter > -1)
                    _fileCounter++;        
                return _fileCounter; 
            }
        }

        // Contrib from hugo on CodePlex - thanks!
        private List<Constraint> GetGroupForeingKeys(List<Constraint> foreignKeys)
        {
            List<Constraint> groupedForeingKeys = new List<Constraint>();

            var uniqueConstaints = (from c in foreignKeys
                                    select c.ConstraintName).Distinct();

            foreach (string item in uniqueConstaints)
            {
                var constraints = (from c in foreignKeys
                                   where c.ConstraintName.Equals(item, StringComparison.Ordinal)
                                   select c).ToList();

                if (constraints.Count == 1)
                {
                    groupedForeingKeys.Add(constraints[0]);
                }
                else
                {
                    Constraint newConstraint = new Constraint();
                    newConstraint.ConstraintTableName = constraints[0].ConstraintTableName;
                    newConstraint.ConstraintName = constraints[0].ConstraintName;
                    newConstraint.UniqueConstraintTableName = constraints[0].UniqueConstraintTableName;
                    newConstraint.UniqueConstraintName = constraints[0].UniqueConstraintName;

                    StringBuilder columnNames = new StringBuilder();
                    StringBuilder uniqueColumnsNames = new StringBuilder();
                    foreach (Constraint c in constraints)
                    {
                        columnNames.Append(c.ColumnName).Append(", ");
                        uniqueColumnsNames.Append(c.UniqueColumnName).Append(", ");
                    }

                    columnNames.Remove(columnNames.Length - 2, 2);
                    uniqueColumnsNames.Remove(uniqueColumnsNames.Length - 2, 2);

                    newConstraint.ColumnName = columnNames.ToString();
                    newConstraint.UniqueColumnName = uniqueColumnsNames.ToString();

                    groupedForeingKeys.Add(newConstraint);
                }
            }
            return groupedForeingKeys;
        }

        private string GetInsertScriptPrefix(string tableName, DataTable dt)
        {
            StringBuilder sbScriptTemplate = new StringBuilder(1000);
            Int32 rowVersionOrdinal = _repository.GetRowVersionOrdinal(tableName);
            sbScriptTemplate.AppendFormat("Insert Into [{0}] (", tableName);

            StringBuilder columnNames = new StringBuilder();
            // Generate the field names first
            for (int iColumn = 0; iColumn < dt.Columns.Count; iColumn++)
            {
                if (iColumn != rowVersionOrdinal && !dt.Columns[iColumn].ColumnName.StartsWith("__sys"))
                {
                    columnNames.AppendFormat("[{0}]{1}", dt.Columns[iColumn].ColumnName, ",");
                }
            }
            columnNames.Remove(columnNames.Length - 1, 1);
            sbScriptTemplate.Append(columnNames.ToString());
            sbScriptTemplate.AppendFormat(") Values (", tableName);
            return sbScriptTemplate.ToString();
        }
    }
}
