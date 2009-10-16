﻿using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;

namespace ExportSqlCE
{
    public class Generator
    {
        private String _outFile;
        private IRepository _repository;
        private StringBuilder _sbScript;
        private String _sep = "GO" + Environment.NewLine;
        private List<string> _tableNames;
        private Int32 _fileCounter = -1;
        private List<Column> _allColumns;

        internal Generator(IRepository repository, string outFile)
        {
            Init(repository, outFile);
        }

        internal void GenerateAllAndSave(bool includeData)
        {
            GenerateTable();
            if (includeData)
            {
                GenerateTableContent();
            }
            GeneratePrimaryKeys();
            GenerateForeignKeys();
            GenerateIndex();
            Helper.WriteIntoFile(GeneratedScript, _outFile, this.FileCounter);
        }

        internal string GenerateTableScript(string tableName)
        {
            GenerateTableCreate(tableName);
            GeneratePrimaryKeys(tableName);
            GenerateForeignKeys(tableName);
            GenerateIndex(tableName);
            return GeneratedScript;
        }


        internal string GenerateTableData(string tableName)
        {
            GenerateTableContent(tableName);
            return GeneratedScript;
        }

        internal string GeneratedScript
        {
            get { return _sbScript.ToString(); }
        }

        internal int FileCounter
        {
            get
            {
                if (_fileCounter > -1)
                    _fileCounter++;
                return _fileCounter;
            }
        }

        private void Init(IRepository repository, string outFile)
        {
            _outFile = outFile;
            _repository = repository;
            _sbScript = new StringBuilder(10000);
            _tableNames = _repository.GetAllTableNames();
            _allColumns = _repository.GetColumnsFromTable();

            _sbScript.AppendFormat("-- Script Date: {0} {1}  - Generated by ExportSqlCe version {2}", DateTime.Now.ToShortDateString(), DateTime.Now.ToShortTimeString(), System.Reflection.Assembly.GetExecutingAssembly().GetName().Version);
            _sbScript.AppendLine();

            if (!string.IsNullOrEmpty(_outFile))
            {
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
        }


        internal void GenerateTable()
        {
            foreach (string tableName in _tableNames)
                GenerateTableCreate(tableName);

        }

        internal void GenerateTableCreate(string tableName)
        {
            List<Column> columns = _allColumns.Where(c => c.TableName == tableName).ToList();
            if (columns.Count > 0)
            {
                // /****** Object:  Table [dbo].[EventLog]    Script Date: 05/31/2009 10:04:32 ******/

                _sbScript.AppendFormat("CREATE TABLE [{0}] ({1}  ", tableName, Environment.NewLine);

                columns.ForEach(delegate(Column col)
                {
                    string line = string.Empty;
                    switch (col.DataType)
                    {
                        case "nvarchar":
                            line = string.Format(System.Globalization.CultureInfo.InvariantCulture,
                                "[{0}] {1}({2}) {3} {4}"
                                , col.ColumnName
                                , col.DataType
                                , col.CharacterMaxLength
                                , (col.IsNullable == YesNoOption.YES ? "NULL" : "NOT NULL")
                                , (col.ColumnHasDefault ? "DEFAULT " + col.ColumnDefault : string.Empty)
                                );
                            break;
                        case "nchar":
                            line = string.Format(System.Globalization.CultureInfo.InvariantCulture,
                                "[{0}] {1}({2}) {3} {4}"
                                , col.ColumnName
                                , "nchar"
                                , col.CharacterMaxLength
                                , (col.IsNullable == YesNoOption.YES ? "NULL" : "NOT NULL")
                                , (col.ColumnHasDefault ? "DEFAULT " + col.ColumnDefault : string.Empty)
                                );
                            break;
                        case "numeric":
                            line = string.Format(System.Globalization.CultureInfo.InvariantCulture,
                                "[{0}] {1}({2},{3}) {4} {5}"
                                , col.ColumnName
                                , col.DataType
                                , col.NumericPrecision
                                , col.NumericScale
                                , (col.IsNullable == YesNoOption.YES ? "NULL" : "NOT NULL")
                                , (col.ColumnHasDefault ? "DEFAULT " + col.ColumnDefault : string.Empty)
                                );
                            break;
                        case "binary":
                            line = string.Format(System.Globalization.CultureInfo.InvariantCulture,
                                "[{0}] {1}({2}) {3} {4}"
                                , col.ColumnName
                                , col.DataType
                                , col.CharacterMaxLength
                                , (col.IsNullable == YesNoOption.YES ? "NULL" : "NOT NULL")
                                , (col.ColumnHasDefault ? "DEFAULT " + col.ColumnDefault : string.Empty)
                                );
                            break;
                        case "varbinary":
                            line = string.Format(System.Globalization.CultureInfo.InvariantCulture,
                                "[{0}] {1}({2}) {3} {4}"
                                , col.ColumnName
                                , col.DataType
                                , col.CharacterMaxLength
                                , (col.IsNullable == YesNoOption.YES ? "NULL" : "NOT NULL")
                                , (col.ColumnHasDefault ? "DEFAULT " + col.ColumnDefault : string.Empty)
                                );
                            break;
                        default:
                            line = string.Format(System.Globalization.CultureInfo.InvariantCulture,
                                "[{0}] {1} {2} {3} {4}{5}"
                                , col.ColumnName
                                , col.DataType
                                , (col.IsNullable == YesNoOption.YES ? "NULL" : "NOT NULL")
                                , (col.ColumnHasDefault ? "DEFAULT " + col.ColumnDefault : string.Empty)
                                , (col.RowGuidCol ? "ROWGUIDCOL" : string.Empty)
                                , (col.AutoIncrementBy > 0 ? string.Format(System.Globalization.CultureInfo.InvariantCulture, "IDENTITY ({0},{1})", col.AutoIncrementSeed, col.AutoIncrementBy) : string.Empty)
                                );
                            break;
                    }
                    _sbScript.AppendFormat("{0}{1}, ",line.Trim(), Environment.NewLine);
                });

                // Remove the last comma
                _sbScript.Remove(_sbScript.Length - 2, 2);
                _sbScript.AppendFormat(");{0}", Environment.NewLine);
                _sbScript.Append(_sep);
            }
        }

        internal void GenerateTableContent()
        {
            foreach (string tableName in _tableNames)
            {
                GenerateTableContent(tableName);
            }
        }

        public void GenerateTableContent(string tableName)
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
            string scriptPrefix = GetInsertScriptPrefix(tableName, fields);

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
                    else if (dt.Columns[iColumn].DataType == typeof(String))
                    {
                        _sbScript.AppendFormat("N'{0}'", dt.Rows[iRow][iColumn].ToString().Replace("'", "''"));
                    }
                    else if (dt.Columns[iColumn].DataType == typeof(DateTime))
                    {
                        DateTime date = (DateTime)dt.Rows[iRow][iColumn];
                        //Datetime globalization - ODBC escape: {ts '2004-03-29 19:21:00'}
                        _sbScript.Append("{ts '");
                        _sbScript.Append(date.ToString("yyyy-MM-dd HH:mm:ss", System.Globalization.CultureInfo.InvariantCulture));
                        _sbScript.Append("'}");
                    }
                    else if (dt.Columns[iColumn].DataType == typeof(Byte[]))
                    {
                        Byte[] buffer = (Byte[])dt.Rows[iRow][iColumn];
                        _sbScript.Append("0x");
                        for (int i = 0; i < buffer.Length; i++)
                        {
                            _sbScript.Append(buffer[i].ToString("X2", System.Globalization.CultureInfo.InvariantCulture));
                        }
                    }
                    else if (dt.Columns[iColumn].DataType == typeof(Byte) || dt.Columns[iColumn].DataType == typeof(Int16) || dt.Columns[iColumn].DataType == typeof(Int32) || dt.Columns[iColumn].DataType == typeof(Int64) || dt.Columns[iColumn].DataType == typeof(Double) || dt.Columns[iColumn].DataType == typeof(Single) || dt.Columns[iColumn].DataType == typeof(Decimal))
                    {
                        string intString = Convert.ToString(dt.Rows[iRow][iColumn], System.Globalization.CultureInfo.InvariantCulture);
                        _sbScript.Append(intString);
                    }
                    else if (dt.Columns[iColumn].DataType == typeof(Boolean))
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
            if (_sbScript.Length > 10485760 && !string.IsNullOrEmpty(_outFile))
            {
                _fileCounter++;
                Helper.WriteIntoFile(_sbScript.ToString(), _outFile, _fileCounter);
                _sbScript.Remove(0, _sbScript.Length);
            }
        }

        internal void GenerateTableContent2()
        {
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

        }

        internal void GenerateTableSelect(string tableName)
        {
            List<Column> columns = _allColumns.Where(c => c.TableName == tableName).ToList();
            if (columns.Count > 0)
            {
                _sbScript.AppendFormat("SELECT ", tableName);

                columns.ForEach(delegate(Column col)
                {
                    _sbScript.AppendFormat(System.Globalization.CultureInfo.InvariantCulture,
                        "[{0}]{1}      ,"
                        , col.ColumnName
                        , Environment.NewLine);
                });

                // Remove the last comma and spaces
                _sbScript.Remove(_sbScript.Length - 7, 7);
                _sbScript.AppendFormat("  FROM [{0}]{1}", tableName, Environment.NewLine);
                _sbScript.Append(_sep);
            }
        }

        internal void GenerateTableInsert(string tableName)
        {
            List<Column> columns = _allColumns.Where(c => c.TableName == tableName).ToList();
            if (columns.Count > 0)
            {
                _sbScript.AppendFormat("INSERT INTO [{0}]", tableName);
                _sbScript.AppendFormat(Environment.NewLine);
                _sbScript.Append("           (");

                columns.ForEach(delegate(Column col)
                {
                    _sbScript.AppendFormat(System.Globalization.CultureInfo.InvariantCulture,
                        "[{0}]{1}           ,"
                        , col.ColumnName
                        , Environment.NewLine);
                });

                // Remove the last comma
                _sbScript.Remove(_sbScript.Length - 14, 14);
                _sbScript.AppendFormat("){0}     VALUES{1}           (", Environment.NewLine, Environment.NewLine);
                columns.ForEach(delegate(Column col)
                {
                    _sbScript.AppendFormat(System.Globalization.CultureInfo.InvariantCulture,
                        "<{0}, {1}>{2}           ,"
                        , col.ColumnName
                        , col.ShortType
                        , Environment.NewLine);
                });
                // Remove the last comma
                _sbScript.Remove(_sbScript.Length - 14, 14);
                _sbScript.AppendFormat(");{0}", Environment.NewLine);
                _sbScript.Append(_sep);
            }
        }

        internal void GenerateTableInsert(string tableName, IList<string> fields, IList<string> values)
        {
            List<Column> columns = _allColumns.Where(c => c.TableName == tableName).ToList();
            if (columns.Count > 0)
            {
                _sbScript.AppendFormat("INSERT INTO [{0}] (", tableName);

                foreach (string field in fields)
                {
                    _sbScript.AppendFormat(System.Globalization.CultureInfo.InvariantCulture,
                        "[{0}],", field);                
                }
                // Remove the last comma
                _sbScript.Remove(_sbScript.Length - 1, 1);
                _sbScript.Append(") VALUES (");
                int i = 0;
                foreach (string value in values)
                {
                    Column column = _allColumns.Where(c => c.TableName == tableName && c.ColumnName == fields[i]).Single();
                    if (string.IsNullOrEmpty(value))
                    {
                        _sbScript.Append("null");
                    }
                    else if (column.DataType == "nchar" || column.DataType == "nvarchar" || column.DataType == "ntext")
                    {
                        _sbScript.AppendFormat("N'{0}'", value.Replace("'", "''"));
                    }
                    else if (column.DataType == "datetime")
                    {
                        DateTime date = DateTime.Parse(value);
                        //Datetime globalization - ODBC escape: {ts '2004-03-29 19:21:00'}
                        _sbScript.Append("{ts '");
                        _sbScript.Append(date.ToString("yyyy-MM-dd HH:mm:ss", System.Globalization.CultureInfo.InvariantCulture));
                        _sbScript.Append("'}");
                    }
                    else if (column.DataType == "bit")
                    {
                        bool boolVal = Boolean.Parse(value);
                        if (boolVal)
                        { _sbScript.Append("1"); }
                        else
                        { _sbScript.Append("0"); }
                    }
                    else
                    {
                        //Decimal point globalization
                        string val = Convert.ToString(value, System.Globalization.CultureInfo.InvariantCulture);
                        _sbScript.AppendFormat("'{0}'", val.Replace("'", "''"));
                    }
                    _sbScript.Append(",");
                    i++;
                }
                // Remove the last comma
                _sbScript.Remove(_sbScript.Length - 1, 1);
                _sbScript.AppendFormat(");{0}", Environment.NewLine);
                _sbScript.Append(_sep);
            }
        }

        internal bool ValidColumns(string tableName, IList<string> columns)
        {
            List<Column> cols = (from a in _allColumns
                                where a.TableName == tableName
                                && columns.Contains(a.ColumnName)
                                select a).ToList();
            if (cols.Count == columns.Count)
            {
                return true;
            }
            else
            {
                _sbScript.Append("-- Cannot create script, one or more field names on first line are invalid");
                return false;
            }
        }

        internal void GenerateTableUpdate(string tableName)
        {
            List<Column> columns = _allColumns.Where(c => c.TableName == tableName).ToList();
            if (columns.Count > 0)
            {
                _sbScript.AppendFormat("UPDATE [{0}] ", tableName);
                _sbScript.AppendFormat(Environment.NewLine);
                _sbScript.Append("   SET ");

                columns.ForEach(delegate(Column col)
                {
                    _sbScript.AppendFormat(System.Globalization.CultureInfo.InvariantCulture,
                        "[{0}] = <{1}, {2}>{3}      ,"
                        , col.ColumnName
                        , col.ColumnName
                        , col.ShortType
                        , Environment.NewLine);
                });

                // Remove the last comma
                _sbScript.Remove(_sbScript.Length - 7, 7);
                _sbScript.AppendFormat(" WHERE <Search Conditions,,>;{0}", Environment.NewLine);
                _sbScript.Append(_sep);
            }
        }

        internal void GenerateTableDelete(string tableName)
        {
            _sbScript.AppendFormat("DELETE FROM [{0}]{1}", tableName, Environment.NewLine);
            _sbScript.AppendFormat("WHERE <Search Conditions,,>;{0}", Environment.NewLine); 
            _sbScript.Append(_sep);
        }

        internal void GenerateTableDrop(string tableName)
        {
            _sbScript.AppendFormat("DROP TABLE [{0}];{1}", tableName, Environment.NewLine);
            _sbScript.Append(_sep);
        }

        internal void GeneratePrimaryKeys()
        {
            foreach(string tableName in _tableNames)
            {
                GeneratePrimaryKeys(tableName);
            }
        }

        public void GeneratePrimaryKeys(string tableName)
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
        }

        internal void GenerateForeignKeys()
        {
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
        
        }

        public void GenerateForeignKeys(string tabelName)
        {
            List<Constraint> foreignKeys = _repository.GetAllForeignKeys(tabelName);
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

        }

        /// <summary>
        /// Added at 18 September 2008, based on Todd Fulmino's comment on 28 August 2008, gosh it's almost a month man :P
        /// </summary>
        /// <returns></returns>
        internal void GenerateIndex()
        {
            foreach (string tableName in _tableNames)
            {
                GenerateIndex(tableName);
            }
        }

        private void GenerateIndex(string tableName)
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
            sbScriptTemplate.AppendFormat("INSERT INTO [{0}] (", tableName);

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
            sbScriptTemplate.Append(") VALUES (");
            return sbScriptTemplate.ToString();
        }

        internal List<string> GenerateTableColumns(string tableName)
        {
            return  (from a in _allColumns
                    where a.TableName == tableName
                    select a.ColumnName).ToList();
        }
    }
}
