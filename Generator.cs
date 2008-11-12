using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;

namespace ExportSQLCE
{
    class Generator
    {
        private IRepository _repository;
        private StringBuilder _sbScript;
        private List<string> _tableNames;
        
        public Generator(IRepository repository)
        {
            _repository = repository;
            _sbScript = new StringBuilder(10000);

            // Populate all tablenames
            _tableNames = _repository.GetAllTableNames();
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
                                    , "nvarchar"
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
                            default :
                                _sbScript.AppendFormat("[{0}] {1} {2} {3} {4} {5}{6}, "
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
                }
            });

            return _sbScript.ToString();
        }
        public string GenerateTableContent()
        {
            Console.WriteLine("Generating the data....");
            _tableNames.ForEach(delegate(string tableName)
            {
                DataTable dt = _repository.GetDataFromTable(tableName);
                
                string scriptPrefix = GetInsertScriptPrefix(tableName, dt);
                
                for (int iRow = 0; iRow < dt.Rows.Count; iRow++)
                {
                    _sbScript.Append(scriptPrefix);
                    for (int iColumn = 0; iColumn < dt.Columns.Count; iColumn++)
                    {
                        if (dt.Rows[iRow][iColumn] == System.DBNull.Value)
                            _sbScript.Append("null");                               
                        else
                            _sbScript.AppendFormat("'{0}'", dt.Rows[iRow][iColumn].ToString().Replace("'", "''"));

                        _sbScript.Append(iColumn != dt.Columns.Count - 1 ? "," : "");
                    }
                    _sbScript.Append(");");
                    _sbScript.Append(System.Environment.NewLine);
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
                }
            });

            return _sbScript.ToString();
        }
        public string GenerateForeignKeys()
        {
            Console.WriteLine("Generating the foreign keys....");
            List<Constraint> foreignKeys = _repository.GetAllForeignKeys();

            foreignKeys.ForEach(delegate(Constraint constraint)
            {
                _sbScript.AppendFormat("ALTER TABLE [{0}] ADD CONSTRAINT [{1}] FOREIGN KEY ([{2}]) REFERENCES [{3}]([{4}]);{5}"
                    , constraint.ConstraintTableName
                    , constraint.ConstraintName
                    , constraint.ColumnName
                    , constraint.UniqueConstraintTableName
                    , constraint.UniqueColumnName
                    , System.Environment.NewLine);
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
                    }                    
                }
            });

            return _sbScript.ToString();            
        }

        

        public string GeneratedScript
        {
            get { return _sbScript.ToString(); }
        }

        private string GetInsertScriptPrefix(string tableName, DataTable dt)
        {
            StringBuilder sbScriptTemplate = new StringBuilder(1000);
            sbScriptTemplate.AppendFormat("Insert Into [{0}] (", tableName);

            // Generate the field names first
            for (int iColumn = 0; iColumn < dt.Columns.Count; iColumn++)
            {
                sbScriptTemplate.AppendFormat("[{0}]{1}", dt.Columns[iColumn].ColumnName, (iColumn != dt.Columns.Count - 1 ? "," : ""));
            }

            sbScriptTemplate.AppendFormat(") Values (", tableName);
            return sbScriptTemplate.ToString();
        }
    }
}
