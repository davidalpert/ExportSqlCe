﻿using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;

namespace ExportSqlCE
{
    class Generator
    {
        private readonly String _outFile;
        private readonly IRepository _repository;
        private readonly StringBuilder _sbScript;
        private readonly String _sep = "GO" + Environment.NewLine;
        private readonly List<string> _tableNames;
        private Int32 _fileCounter = -1;
        
        public Generator(IRepository repository, string outFile)
        {
            _outFile = outFile;
            _repository = repository;
            _sbScript = new StringBuilder(10000);

            _sbScript.AppendFormat("-- {0} {1}", DateTime.Now.ToShortDateString(), DateTime.Now.ToShortTimeString());
            _sbScript.AppendLine();

            _sbScript.Append("-- Database information:");
            _sbScript.AppendLine();

            foreach (var kv in _repository.GetDatabaseInfo())
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

        public string GenerateTables()
        {
            Console.WriteLine("Generating the tables....");
            List<Column> allColumns = _repository.GetColumnsFromTable();
            _tableNames.ForEach(delegate(string tableName)
            {
                List<Column> columns = allColumns.Where(c => c.TableName == tableName).ToList();
                if (columns.Count > 0)
                {
                    _sbScript.AppendFormat("CREATE TABLE [{0}] (", tableName);

                    columns.ForEach(delegate(Column col)
                    {
                        switch (col.DataType)
                        {
                            case "nvarchar":
                                _sbScript.AppendFormat(System.Globalization.CultureInfo.InvariantCulture,
                                    "[{0}] {1}({2}) {3} {4} {5}, "
                                    , col.ColumnName
                                    , col.DataType
                                    , col.CharacterMaxLength
                                    , (col.IsNullable == YesNoOption.YES ? "NULL" : "NOT NULL")
                                    , (col.ColumnHasDefault ? "DEFAULT " + col.ColumnDefault : string.Empty)
                                    , Environment.NewLine);
                                break;
                            case "nchar":
                                _sbScript.AppendFormat(System.Globalization.CultureInfo.InvariantCulture,
                                    "[{0}] {1}({2}) {3} {4} {5}, "
                                    , col.ColumnName
                                    , "nchar"
                                    , col.CharacterMaxLength
                                    , (col.IsNullable == YesNoOption.YES ? "NULL" : "NOT NULL")
                                    , (col.ColumnHasDefault ? "DEFAULT " + col.ColumnDefault : string.Empty)
                                    , Environment.NewLine);
                                break;
                            case "numeric":
                                _sbScript.AppendFormat(System.Globalization.CultureInfo.InvariantCulture,
                                    "[{0}] {1}({2},{3}) {4} {5} {6}, "
                                    , col.ColumnName
                                    , col.DataType
                                    , col.NumericPrecision
                                    , col.NumericScale
                                    , (col.IsNullable == YesNoOption.YES ? "NULL" : "NOT NULL")
                                    , (col.ColumnHasDefault ? "DEFAULT " + col.ColumnDefault : string.Empty)
                                    , Environment.NewLine);
                                break;
                            case "binary":
                                _sbScript.AppendFormat(System.Globalization.CultureInfo.InvariantCulture,
                                    "[{0}] {1}({2}) {3} {4} {5}, "
                                    , col.ColumnName
                                    , col.DataType
                                    , col.CharacterMaxLength
                                    , (col.IsNullable == YesNoOption.YES ? "NULL" : "NOT NULL")
                                    , (col.ColumnHasDefault ? "DEFAULT " + col.ColumnDefault : string.Empty)
                                    , Environment.NewLine);
                                break;
                            case "varbinary":
                                _sbScript.AppendFormat(System.Globalization.CultureInfo.InvariantCulture,
                                    "[{0}] {1}({2}) {3} {4} {5}, "
                                    , col.ColumnName
                                    , col.DataType
                                    , col.CharacterMaxLength                                    
                                    , (col.IsNullable == YesNoOption.YES ? "NULL" : "NOT NULL")
                                    , (col.ColumnHasDefault ? "DEFAULT " + col.ColumnDefault : string.Empty)
                                    , Environment.NewLine);
                                break;
                            default:
                                _sbScript.AppendFormat(System.Globalization.CultureInfo.InvariantCulture,
                                    "[{0}] {1} {2} {3} {4}{5} {6}, "
                                    , col.ColumnName
                                    , col.DataType
                                    , (col.IsNullable == YesNoOption.YES ? "NULL" : "NOT NULL")
                                    , (col.ColumnHasDefault ? "DEFAULT " + col.ColumnDefault : string.Empty)
                                    , (col.RowGuidCol ? "ROWGUIDCOL" : string.Empty)
                                    , (col.AutoIncrementBy > 0 ? string.Format(System.Globalization.CultureInfo.InvariantCulture, "IDENTITY ({0},{1})", col.AutoIncrementSeed, col.AutoIncrementBy) : string.Empty)
                                    , Environment.NewLine);
                                break;
                        }   
                    });

                    // Remove the last comma
                    _sbScript.Remove(_sbScript.Length - 2, 2);
                    _sbScript.AppendFormat(");{0}", Environment.NewLine);
                    _sbScript.Append(_sep);
                }
            });

            return _sbScript.ToString();
        }

        public string GenerateTableContent()
        {
            Console.WriteLine("Generating the data....");
            foreach (string tableName in _tableNames)
            {
                // Skip rowversion column
                Int32 rowVersionOrdinal = _repository.GetRowVersionOrdinal(tableName);
                DataTable dt = _repository.GetDataFromTable(tableName);
                bool hasIdentity = _repository.HasIdentityColumn(tableName);
#if V35
                if (hasIdentity)
                {
                    _sbScript.Append(string.Format(System.Globalization.CultureInfo.InvariantCulture, "SET IDENTITY_INSERT [{0}] ON", tableName));
                    _sbScript.Append(Environment.NewLine);
                    _sbScript.Append(_sep);
                }
#endif         
                var fields = new List<string>();
                for (int iColumn = 0; iColumn < dt.Columns.Count; iColumn++)
                {
                    fields.Add(dt.Columns[iColumn].ColumnName);
                }
                string scriptPrefix = GetInsertScriptPrefix(tableName,fields);

                for (int iRow = 0; iRow < dt.Rows.Count; iRow++)
                {
                    _sbScript.Append(scriptPrefix);
                    for (int iColumn = 0; iColumn < dt.Columns.Count; iColumn++)
                    {
                        
                        //Skip rowversion column
                        if (rowVersionOrdinal == iColumn || dt.Columns[iColumn].ColumnName.StartsWith("__sys", StringComparison.OrdinalIgnoreCase))
                        {
                            continue;
                        }
                        if (dt.Rows[iRow][iColumn] == DBNull.Value)
                        {
                            _sbScript.Append("null");
                        }
                        else if (dt.Columns[iColumn].DataType == typeof (String))
                        {
                            _sbScript.AppendFormat("N'{0}'", dt.Rows[iRow][iColumn].ToString().Replace("'", "''"));
                        }
                        else if (dt.Columns[iColumn].DataType == typeof (DateTime))
                        {
                            DateTime date = (DateTime)dt.Rows[iRow][iColumn];
                            //Datetime globalization - ODBC escape: {ts '2004-03-29 19:21:00'}
                            _sbScript.Append("{ts '");
                            _sbScript.Append(date.ToString("yyyy-MM-dd HH:mm:ss", System.Globalization.CultureInfo.InvariantCulture));
                            _sbScript.Append("'}");
                        }
                        else if (dt.Columns[iColumn].DataType == typeof (Byte[]))
                        {
                            Byte[] buffer = (Byte[])dt.Rows[iRow][iColumn];
                            _sbScript.Append("0x");
                            for (int i = 0; i < buffer.Length; i++)
                            {
                                _sbScript.Append(buffer[i].ToString("X2", System.Globalization.CultureInfo.InvariantCulture));
                            }
                        }
                        else if (dt.Columns[iColumn].DataType == typeof (Byte) || dt.Columns[iColumn].DataType == typeof (Int16) || dt.Columns[iColumn].DataType == typeof (Int32) || dt.Columns[iColumn].DataType == typeof (Int64) || dt.Columns[iColumn].DataType == typeof(Double) || dt.Columns[iColumn].DataType == typeof (Single) || dt.Columns[iColumn].DataType == typeof (Decimal))
                        {
                            string intString = Convert.ToString(dt.Rows[iRow][iColumn], System.Globalization.CultureInfo.InvariantCulture);
                            _sbScript.Append(intString);
                        }
                        else if (dt.Columns[iColumn].DataType == typeof (Boolean))
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
                        _sbScript.Append(",");
                    }
                    // remove trailing comma
                    _sbScript.Remove(_sbScript.Length - 1, 1);

                    _sbScript.Append(");");
                    _sbScript.Append(Environment.NewLine);
                    _sbScript.Append(_sep);
                }
#if V35
                if (hasIdentity)
                {
                    _sbScript.Append(string.Format(System.Globalization.CultureInfo.InvariantCulture, "SET IDENTITY_INSERT [{0}] OFF", tableName));
                    _sbScript.Append(Environment.NewLine);
                    _sbScript.Append(_sep);
                }
#endif
                if (_sbScript.Length > 10485760)
                {
                    _fileCounter++;
                    Helper.WriteIntoFile(_sbScript.ToString(), _outFile, _fileCounter);
                    _sbScript.Remove(0, _sbScript.Length);
                }
            }

            return _sbScript.ToString();
        }

        public string GenerateTableContent2()
        {
            Console.WriteLine("Generating the data....");
            foreach (string tableName in _tableNames)
            {
                bool hasIdentity = _repository.HasIdentityColumn(tableName);
                // Skip rowversion column
                Int32 rowVersionOrdinal = _repository.GetRowVersionOrdinal(tableName);
                using (IDataReader dr = _repository.GetDataFromReader(tableName))
                {
#if V35
                    if (hasIdentity)
                    {
                        _sbScript.Append(string.Format(System.Globalization.CultureInfo.InvariantCulture, "SET IDENTITY_INSERT [{0}] ON", tableName));
                        _sbScript.Append(Environment.NewLine);
                        _sbScript.Append(_sep);
                    }
#endif
                    var fields = new List<string>();
                    for (int iColumn = 0; iColumn < dr.FieldCount; iColumn++)
                    {
                        fields.Add(dr.GetName(iColumn));
                    }
                    string scriptPrefix = GetInsertScriptPrefix(tableName, fields);
                    while (dr.Read())
                    {
                        _sbScript.Append(scriptPrefix);
                        for (int iColumn = 0; iColumn < dr.FieldCount; iColumn++)
                        {

                            //Skip rowversion column
                            if (rowVersionOrdinal == iColumn ||  dr.GetName(iColumn).StartsWith("__sys", StringComparison.OrdinalIgnoreCase))
                            {
                                continue;
                            }
                            if (dr.GetValue(iColumn) == DBNull.Value)
                            {
                                _sbScript.Append("null");
                            }
                            else if (dr.GetValue(iColumn).GetType() == typeof (String))
                            {
                                _sbScript.AppendFormat("N'{0}'", dr[iColumn].ToString().Replace("'", "''"));
                            }
                            else if (dr.GetValue(iColumn).GetType() == typeof (DateTime))
                            {
                                DateTime date = (DateTime)dr[iColumn];
                                //Datetime globalization - ODBC escape: {ts '2004-03-29 19:21:00'}
                                _sbScript.Append("{ts '");
                                _sbScript.Append(date.ToString("yyyy-MM-dd HH:mm:ss", System.Globalization.CultureInfo.InvariantCulture));
                                _sbScript.Append("'}");
                            }
                            else if (dr.GetValue(iColumn).GetType() == typeof (Byte[]))
                            {
                                Byte[] buffer = (Byte[])dr[iColumn];
                                _sbScript.Append("0x");
                                for (int i = 0; i < buffer.Length; i++)
                                {
                                    _sbScript.Append(buffer[i].ToString("X2", System.Globalization.CultureInfo.InvariantCulture));
                                }
                            }
                            else if (dr.GetValue(iColumn).GetType() == typeof(Byte) || dr.GetValue(iColumn).GetType() == typeof(Int16) || dr.GetValue(iColumn).GetType() == typeof(Int32) || dr.GetValue(iColumn).GetType() == typeof(Int64) || dr.GetValue(iColumn).GetType() == typeof(Double) || dr.GetValue(iColumn).GetType() == typeof(Single) || dr.GetValue(iColumn).GetType() == typeof(Decimal))
                            {
                                string intString = Convert.ToString(dr[iColumn], System.Globalization.CultureInfo.InvariantCulture);
                                _sbScript.Append(intString);
                            }
                            else if (dr.GetValue(iColumn).GetType() == typeof(Boolean))
                            {
                                bool boolVal = (Boolean)dr[iColumn];
                                if (boolVal)
                                { _sbScript.Append("1"); }
                                else
                                { _sbScript.Append("0"); }
                            }
                            else
                            {
                                //Decimal point globalization
                                string value = Convert.ToString(dr[iColumn], System.Globalization.CultureInfo.InvariantCulture);
                                _sbScript.AppendFormat("'{0}'", value.Replace("'", "''"));
                            }
                            _sbScript.Append(",");
                        }
                        // remove trailing comma
                        _sbScript.Remove(_sbScript.Length - 1, 1);

                        _sbScript.Append(");");
                        _sbScript.Append(Environment.NewLine);
                        _sbScript.Append(_sep);
                    }
                }
#if V35
                if (hasIdentity)
                {
                    _sbScript.Append(string.Format(System.Globalization.CultureInfo.InvariantCulture, "SET IDENTITY_INSERT [{0}] OFF", tableName));
                    _sbScript.Append(Environment.NewLine);
                    _sbScript.Append(_sep);
                }
#endif
                if (_sbScript.Length > 10485760)
                {
                    _fileCounter++;
                    Helper.WriteIntoFile(_sbScript.ToString(), _outFile, _fileCounter);
                    _sbScript.Remove(0, _sbScript.Length);
                }
            }

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
                    _sbScript.AppendFormat(");{0}", Environment.NewLine);
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
            List<Constraint> foreingKeysGrouped = GetGroupForeingKeys(foreignKeys);

            foreingKeysGrouped.ForEach(delegate(Constraint constraint)
            {
                _sbScript.AppendFormat(System.Globalization.CultureInfo.InvariantCulture, "ALTER TABLE [{0}] ADD CONSTRAINT [{1}] FOREIGN KEY ({2}) REFERENCES [{3}]({4}) ON DELETE {5} ON UPDATE {6};{7}"
                    , constraint.ConstraintTableName
                    , constraint.ConstraintName
                    , constraint.ColumnName
                    , constraint.UniqueConstraintTableName
                    , constraint.UniqueColumnName
                    , constraint.DeleteRule
                    , constraint.UpdateRule
                    , Environment.NewLine);
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
                        string name = uniqueIndexName;
                        IOrderedEnumerable<Index> indexesByName = from i in tableIndexes
                                                                  where i.IndexName == name
                                                                  orderby i.OrdinalPosition
                                                                  select i;

                        _sbScript.Append("CREATE ");

                        // Just get the first one to decide whether it's unique and/or clustered index
                        var idx = indexesByName.First();
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


        // Contrib from hugo on CodePlex - thanks!
        private static List<Constraint> GetGroupForeingKeys(List<Constraint> foreignKeys)
        {
            var groupedForeingKeys = new List<Constraint>();

            var uniqueConstaints = (from c in foreignKeys
                                    select c.ConstraintName).Distinct();

            foreach (string item in uniqueConstaints)
            {
                string value = item;
                var constraints = foreignKeys.Where(c => c.ConstraintName.Equals(value, StringComparison.Ordinal)).ToList();

                if (constraints.Count == 1)
                {
                    groupedForeingKeys.Add(constraints[0]);
                }
                else
                {
                    var newConstraint = new Constraint { ConstraintTableName = constraints[0].ConstraintTableName, ConstraintName = constraints[0].ConstraintName, UniqueConstraintTableName = constraints[0].UniqueConstraintTableName, UniqueConstraintName = constraints[0].UniqueConstraintName, DeleteRule = constraints[0].DeleteRule, UpdateRule = constraints[0].UpdateRule };
                    var columnNames = new StringBuilder();
                    var uniqueColumnsNames = new StringBuilder();
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

        private string GetInsertScriptPrefix(string tableName, List<string> fieldNames)
        {
            StringBuilder sbScriptTemplate = new StringBuilder(1000);
            Int32 rowVersionOrdinal = _repository.GetRowVersionOrdinal(tableName);
            sbScriptTemplate.AppendFormat("Insert Into [{0}] (", tableName);

            StringBuilder columnNames = new StringBuilder();
            // Generate the field names first
            for (int iColumn = 0; iColumn < fieldNames.Count; iColumn++)
            {
                if (iColumn != rowVersionOrdinal && !fieldNames[iColumn].StartsWith("__sys", StringComparison.OrdinalIgnoreCase))
                {
                    columnNames.AppendFormat("[{0}]{1}", fieldNames[iColumn], ",");
                }
            }
            columnNames.Remove(columnNames.Length - 1, 1);
            sbScriptTemplate.Append(columnNames.ToString());
            sbScriptTemplate.Append(") Values (");
            return sbScriptTemplate.ToString();
        }
    }
}
