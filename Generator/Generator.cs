using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Text;

namespace ErikEJ.SqlCeScripting
{
    /// <summary>
    /// Class for generating scripts
    /// Use the GeneratedScript property to get the resulting script
    /// </summary>
#if V40
    public class Generator4 : IGenerator
#else
    public class Generator : IGenerator
#endif
    {
        private String _outFile;
        private IRepository _repository;
        private StringBuilder _sbScript;
        private String _sep = "GO" + Environment.NewLine;
        private List<string> _tableNames;
        private Int32 _fileCounter = -1;
        private List<Column> _allColumns;
        private List<Constraint> _allForeignKeys;
        private List<PrimaryKey> _allPrimaryKeys;
        private bool _batchForAzure = false;

        /// <summary>
        /// Initializes a new instance of the <see cref="Generator"/> class.
        /// </summary>
        /// <param name="repository">The repository.</param>
        /// <param name="outFile">The out file.</param>
#if V40
        public Generator4(IRepository repository, string outFile)
#else
        public Generator(IRepository repository, string outFile)
#endif
        {
            Init(repository, outFile);
        }

#if V40
        public Generator4(IRepository repository)
#else
        public Generator(IRepository repository)
#endif
        {
            Init(repository, null);
        }

#if V40
        public Generator4(IRepository repository, string outFile, bool azure)
#else

        public Generator(IRepository repository, string outFile, bool azure)
#endif
        {
            _batchForAzure = azure;
            Init(repository, outFile);
        }

        public string ScriptDatabaseToFile(Scope scope)
        {
            Helper.FinalFiles = _outFile;
            System.Diagnostics.Stopwatch sw = new System.Diagnostics.Stopwatch();
            sw.Start();
            switch (scope)
            {
                case Scope.Schema:
                    GenerateAllAndSave(false, false);
                    break;
                case Scope.SchemaData:
                    GenerateAllAndSave(true, false);
                    break;
                case Scope.SchemaDataBlobs:
                    GenerateAllAndSave(true, true);
                    break;
                case Scope.SchemaDataAzure:
                    _batchForAzure = true;
                    GenerateAllAndSave(true, false);
                    break;                    
                default:
                    break;
            }
            sw.Stop();
            return string.Format(System.Globalization.CultureInfo.InvariantCulture, "Sent script to output file(s) : {0} in {1} ms", Helper.FinalFiles, (sw.ElapsedMilliseconds).ToString(System.Globalization.CultureInfo.CurrentCulture));
        }

        /// <summary>
        /// Generates the table script.
        /// </summary>
        /// <param name="tableName">Name of the table.</param>
        /// <returns></returns>
        public void GenerateTableScript(string tableName)
        {
            GenerateTableCreate(tableName, false);
            GeneratePrimaryKeys(tableName);
            GenerateIndex(tableName);
            GenerateForeignKeys(tableName);
        }

        /// <summary>
        /// Generates the table data.
        /// </summary>
        /// <param name="tableName">Name of the table.</param>
        /// <param name="saveImageFiles">if set to <c>true</c> [save image files].</param>
        /// <returns></returns>
        public string GenerateTableData(string tableName, bool saveImageFiles)
        {
            GenerateTableContent(tableName, saveImageFiles);
            return GeneratedScript;
        }

        /// <summary>
        /// Gets the generated script.
        /// </summary>
        /// <value>The generated script.</value>
        public string GeneratedScript
        {
            get { return _sbScript.ToString(); }
        }

        /// <summary>
        /// Generates the content of the table.
        /// </summary>
        /// <param name="tableName">Name of the table.</param>
        /// <param name="saveImageFiles">if set to <c>true</c> [save image files].</param>
        public void GenerateTableContent(string tableName, bool saveImageFiles)
        {
            // Skip rowversion column
            Int32 rowVersionOrdinal = _repository.GetRowVersionOrdinal(tableName);
            List<Column> columns = _allColumns.Where(c => c.TableName == tableName).ToList();
            using (DataTable dt = _repository.GetDataFromTable(tableName, columns))
            {
                bool hasIdentity = _repository.HasIdentityColumn(tableName);
#if V31
#else
                if (hasIdentity && dt.Rows.Count > 0)
                {
                    _sbScript.Append(string.Format(System.Globalization.CultureInfo.InvariantCulture, "SET IDENTITY_INSERT [{0}] ON;", tableName));
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
                            // see http://msdn.microsoft.com/en-us/library/ms180878.aspx#BackwardCompatibilityforDownlevelClients
                            Column column = _allColumns.Where(c => c.TableName == tableName && c.ColumnName == dt.Columns[iColumn].ColumnName).Single();
                            DateTime date = (DateTime)dt.Rows[iRow][iColumn];
                            switch (column.DateFormat)
                            {
                                case DateFormat.None:
                                    //Datetime globalization - ODBC escape: {ts '2004-03-29 19:21:00'}
                                    _sbScript.Append("{ts '");
                                    _sbScript.Append(date.ToString("yyyy-MM-dd HH:mm:ss.fff", System.Globalization.CultureInfo.InvariantCulture));
                                    _sbScript.Append("'}");
                                    break;
                                case DateFormat.DateTime:
                                    //Datetime globalization - ODBC escape: {ts '2004-03-29 19:21:00'}
                                    _sbScript.Append("{ts '");
                                    _sbScript.Append(date.ToString("yyyy-MM-dd HH:mm:ss.fff", System.Globalization.CultureInfo.InvariantCulture));
                                    _sbScript.Append("'}");
                                    break;
                                case DateFormat.Date:
                                    _sbScript.Append("N'");
                                    _sbScript.Append(date.ToString("yyyy-MM-dd", System.Globalization.CultureInfo.InvariantCulture));
                                    _sbScript.Append("'");
                                    break;
                                case DateFormat.DateTime2:
                                    _sbScript.Append("N'");
                                    _sbScript.Append(date.ToString("yyyy-MM-dd HH:mm:ss.fffffff", System.Globalization.CultureInfo.InvariantCulture));
                                    _sbScript.Append("'");
                                    break;
                            }

                        }
                        else if (dt.Columns[iColumn].DataType == typeof(DateTimeOffset))
                        {
                            DateTimeOffset dto = (DateTimeOffset)dt.Rows[iRow][iColumn];
                            _sbScript.Append("N'");
                            _sbScript.Append(dto.ToString("yyyy-MM-dd HH:mm:ss.fffffff zzz", System.Globalization.CultureInfo.InvariantCulture));
                            _sbScript.Append("'");
                        }
                        else if (dt.Columns[iColumn].DataType == typeof(TimeSpan))
                        {
                            TimeSpan ts = (TimeSpan)dt.Rows[iRow][iColumn];
                            _sbScript.Append("N'");
                            _sbScript.Append(ts.ToString());
                            _sbScript.Append("'");
                        }
                        else if (dt.Columns[iColumn].DataType == typeof(Byte[]))
                        {
                            Byte[] buffer = (Byte[])dt.Rows[iRow][iColumn];
                            if (saveImageFiles)
                            {
                                string id = Guid.NewGuid().ToString("N") + ".blob";
                                _sbScript.AppendFormat("SqlCeCmd_LoadImage({0})", id);
                                using (BinaryWriter bw = new BinaryWriter(File.Open(Path.Combine(Path.GetDirectoryName(_outFile), id), FileMode.Create)))
                                {
                                    bw.Write(buffer, 0, buffer.Length);
                                }
                            }
                            else
                            {
                                _sbScript.Append("0x");
                                for (int i = 0; i < buffer.Length; i++)
                                {
                                    _sbScript.Append(buffer[i].ToString("X2", System.Globalization.CultureInfo.InvariantCulture));
                                }
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
                    if (_batchForAzure && ((iRow + 1) % 1000) == 0) 
                    {
                        _sbScript.Append(_sep);
                    }
                    else if (!_batchForAzure)
                    {
                        _sbScript.Append(_sep);
                    }
                    // Split large output!
                    if (_sbScript.Length > 9485760 && !string.IsNullOrEmpty(_outFile))
                    {
                        if (_batchForAzure)
                        {
                            _sbScript.Append(_sep);
                        }
#if V31
#else
                        if (hasIdentity && dt.Rows.Count > 0)
                        {
                            _sbScript.Append(string.Format(System.Globalization.CultureInfo.InvariantCulture, "SET IDENTITY_INSERT [{0}] OFF;", tableName));
                            _sbScript.Append(Environment.NewLine);
                            _sbScript.Append(_sep);
                        }
#endif

                        _fileCounter++;
                        Helper.WriteIntoFile(_sbScript.ToString(), _outFile, _fileCounter);
                        _sbScript.Remove(0, _sbScript.Length);
#if V31
#else
                        if (hasIdentity && dt.Rows.Count > 0)
                        {
                            _sbScript.Append(string.Format(System.Globalization.CultureInfo.InvariantCulture, "SET IDENTITY_INSERT [{0}] ON;", tableName));
                            _sbScript.Append(Environment.NewLine);
                            _sbScript.Append(_sep);
                        }
#endif
                    }
                }
                if (_batchForAzure)
                {
                    _sbScript.Append(_sep);
                }
#if V31
#else
                if (hasIdentity && dt.Rows.Count > 0)
                {
                    _sbScript.Append(string.Format(System.Globalization.CultureInfo.InvariantCulture, "SET IDENTITY_INSERT [{0}] OFF;", tableName));
                    _sbScript.Append(Environment.NewLine);
                    _sbScript.Append(_sep);
                }
#endif
            }
        }

        /// <summary>
        /// Generates the table select statement.
        /// </summary>
        /// <param name="tableName">Name of the table.</param>
        public void GenerateTableSelect(string tableName)
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

        /// <summary>
        /// Generates the table insert statement.
        /// </summary>
        /// <param name="tableName">Name of the table.</param>
        public void GenerateTableInsert(string tableName)
        {
            List<Column> columns = _allColumns.Where(c => c.TableName == tableName).ToList();
            if (columns.Count > 0)
            {
                _sbScript.AppendFormat(System.Globalization.CultureInfo.InvariantCulture, "INSERT INTO [{0}]", tableName);
                _sbScript.AppendFormat(System.Globalization.CultureInfo.InvariantCulture, Environment.NewLine);
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

        /// <summary>
        /// Generates the table insert.
        /// </summary>
        /// <param name="tableName">Name of the table.</param>
        /// <param name="fields">The fields.</param>
        /// <param name="values">The values.</param>
        public void GenerateTableInsert(string tableName, IList<string> fields, IList<string> values)
        {
            if (fields.Count != values.Count)
            {
                StringBuilder valueString = new StringBuilder();
                valueString.Append("Values:");
                valueString.Append(Environment.NewLine);
                foreach (string val in values)
                {
                    valueString.Append(val);
                    valueString.Append(Environment.NewLine);
                }
                throw new ArgumentException(string.Format(System.Globalization.CultureInfo.InvariantCulture, "The number of values ({0}) and fields ({1}) do not match - {2}", values.Count, fields.Count, valueString.ToString()));
            }
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
                    //else if (value == string.Empty)
                    //{
                    //    _sbScript.Append("''");
                    //}
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
                        if (value == "0" || value == "1")
                        {
                            _sbScript.Append(value);
                        }
                        else
                        {
                            bool boolVal = Boolean.Parse(value);
                            if (boolVal)
                            { _sbScript.Append("1"); }
                            else
                            { _sbScript.Append("0"); }
                        }
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

        public void AddIdentityInsert(string tableName)
        {
            bool hasIdentity = _repository.HasIdentityColumn(tableName);
            if (hasIdentity)
            {
                _sbScript.Append(string.Format(System.Globalization.CultureInfo.InvariantCulture, "SET IDENTITY_INSERT [{0}] ON;", tableName));
                _sbScript.Append(Environment.NewLine);
                _sbScript.Append(_sep);
            }
        }

        /// <summary>
        /// Validates the columns.
        /// </summary>
        /// <param name="tableName">Name of the table.</param>
        /// <param name="columns">The columns.</param>
        /// <returns></returns>
        public bool ValidColumns(string tableName, IList<string> columns)
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
                _sbScript.Append(Environment.NewLine);
                _sbScript.Append("-- Also check that correct separator is chosen");
                return false;
            }
        }

        /// <summary>
        /// Generates the table update statement.
        /// </summary>
        /// <param name="tableName">Name of the table.</param>
        public void GenerateTableUpdate(string tableName)
        {
            List<Column> columns = _allColumns.Where(c => c.TableName == tableName).ToList();
            if (columns.Count > 0)
            {
                _sbScript.AppendFormat(System.Globalization.CultureInfo.InvariantCulture, "UPDATE [{0}] ", tableName);
                _sbScript.AppendFormat(System.Globalization.CultureInfo.InvariantCulture, Environment.NewLine);
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

        /// <summary>
        /// Generates the table delete statement.
        /// </summary>
        /// <param name="tableName">Name of the table.</param>
        public void GenerateTableDelete(string tableName)
        {
            _sbScript.AppendFormat("DELETE FROM [{0}]{1}", tableName, Environment.NewLine);
            _sbScript.AppendFormat("WHERE <Search Conditions,,>;{0}", Environment.NewLine); 
            _sbScript.Append(_sep);
        }

        /// <summary>
        /// Generates the table drop statement.
        /// </summary>
        /// <param name="tableName">Name of the table.</param>
        public void GenerateTableDrop(string tableName)
        {
            _sbScript.AppendFormat("DROP TABLE [{0}];{1}", tableName, Environment.NewLine);
            _sbScript.Append(_sep);
        }

        public void GenerateSchemaGraph(string connectionString)
        {
            GenerateSchemaGraph(connectionString, true);
        }

        /// <summary>
        /// Generates the schema graph.
        /// </summary>
        public void GenerateSchemaGraph(string connectionString, bool includeSystemTables)
        {
            string dgmlFile = _outFile;
            var dgmlHelper = new DgmlHelper(dgmlFile);

            string scriptExt = ".dgml.sqlce";
            if (_repository.IsServer())
            {
                scriptExt = ".dgml.sql";
            }

            var descriptionHelper = new DescriptionHelper();

            List<DbDescription> descriptionCache = descriptionHelper.GetDescriptions(_repository);

            dgmlHelper.BeginElement("Nodes");
            var dbdesc = descriptionCache.Where(dc => dc.Parent == null && dc.Object == null).Select(dc => dc.Description).SingleOrDefault();
            dgmlHelper.WriteNode("Database", connectionString, null, "Database", "Expanded", dbdesc);
            foreach (string table in _tableNames)
            {
                if (!includeSystemTables && table.StartsWith("__"))
                    continue;
                //Create individual scripts per table
                _sbScript.Remove(0, _sbScript.Length);
                GenerateTableScript(table);
                string tableScriptPath = Path.Combine(Path.GetDirectoryName(dgmlFile), table + scriptExt);
                File.WriteAllText(tableScriptPath, GeneratedScript);
                // Create Nodes
                var desc = descriptionCache.Where(dc => dc.Parent == null && dc.Object == table).Select(dc => dc.Description).SingleOrDefault();
                dgmlHelper.WriteNode(table, table, table + scriptExt, "Table", "Collapsed", desc);
                List<Column> columns = _allColumns.Where(c => c.TableName == table).ToList();
                foreach (Column col in columns)
                {

                    string shortType = col.ShortType.Remove(col.ShortType.Length - 1);

                    string category = "Field";
                    if (col.IsNullable == YesNoOption.YES)
                        category = "Field Optional";

                    // Fix for multiple constraints with same columns
                    Dictionary<string, Constraint> columnForeignKeys = new Dictionary<string, Constraint>();

                    var _tableKeys = _allForeignKeys.Where(c => c.ConstraintTableName == col.TableName);
                    foreach (var constraint in _tableKeys)
                    {
                        if (!columnForeignKeys.ContainsKey(constraint.Columns.ToString()))
                        {
                            columnForeignKeys.Add(constraint.Columns.ToString(), constraint);
                        }
                    }

                    if (columnForeignKeys.ContainsKey(string.Format("[{0}]", col.ColumnName)))
                    {
                        category = "Field Foreign";
                    }

                    List<PrimaryKey> primaryKeys = _allPrimaryKeys.Where(p => p.TableName == table).ToList();
                    if (primaryKeys.Count > 0)
                    {
                        var keys = (from k in primaryKeys
                                   where k.ColumnName == col.ColumnName 
                                   select k.ColumnName).SingleOrDefault();
                        if (!string.IsNullOrEmpty(keys))
                            category = "Field Primary";

                    }
                    var colDesc = descriptionCache.Where(dc => dc.Parent == table && dc.Object == col.ColumnName).Select(dc => dc.Description).SingleOrDefault();
                    if (!string.IsNullOrEmpty(colDesc))
                        shortType = shortType + Environment.NewLine + colDesc;
                    dgmlHelper.WriteNode(string.Format("{0}_{1}", table, col.ColumnName), col.ColumnName, null, category, null, shortType);
                }
            }
            dgmlHelper.EndElement();

            dgmlHelper.BeginElement("Links");
            foreach (string table in _tableNames)
            {
                if (!includeSystemTables && table.StartsWith("__"))
                    continue;

                dgmlHelper.WriteLink("Database", table, null, "Contains");
                
                List<Column> columns = _allColumns.Where(c => c.TableName == table).ToList();
                foreach (Column col in columns)
                {
                    dgmlHelper.WriteLink(table, string.Format("{0}_{1}", table, col.ColumnName), 
                        null, "Contains");
                }

                List<Constraint> foreignKeys = _repository.GetAllForeignKeys(table);
                foreach (Constraint key in foreignKeys)
                {
                    dgmlHelper.WriteLink(string.Format("{0}_{1}", table, key.Columns[0].ToString()), string.Format("{0}_{1}", key.UniqueConstraintTableName, key.UniqueColumns[0].ToString()), key.ConstraintName, "Foreign Key");
                }
            }
            dgmlHelper.EndElement();

            //Close the DGML document
            dgmlHelper.Close();
        }

        internal void GeneratePrimaryKeys()
        {
            foreach(string tableName in _tableNames)
            {
                GeneratePrimaryKeys(tableName);
            }
        }

        /// <summary>
        /// Generates the primary keys.
        /// </summary>
        /// <param name="tableName">Name of the table.</param>
        public void GeneratePrimaryKeys(string tableName)
        {
            List<PrimaryKey> primaryKeys = _allPrimaryKeys.Where(p => p.TableName == tableName).ToList();
            
            //_repository.GetPrimaryKeysFromTable(tableName);

            if (primaryKeys.Count > 0)
            {
                _sbScript.AppendFormat("ALTER TABLE [{0}] ADD CONSTRAINT [{1}] PRIMARY KEY (", tableName, primaryKeys[0].KeyName);

                primaryKeys.ForEach(delegate(PrimaryKey column)
                {
                    _sbScript.AppendFormat("[{0}]", column.ColumnName);
                    _sbScript.Append(",");
                });

                // Remove the last comma
                _sbScript.Remove(_sbScript.Length - 1, 1);
                _sbScript.AppendFormat(");{0}", Environment.NewLine);
                _sbScript.Append(_sep);
            }
            else if (_batchForAzure)
            {
                _sbScript.AppendFormat("PRINT N'** Warning: Table [{0}] does not have a primary clustered key - it cannot be migrated to SQL Azure';{1}", tableName, Environment.NewLine);
            }
        }

        internal void GenerateForeignKeys()
        {
            List<Constraint> foreignKeys = _allForeignKeys;

            foreignKeys.ForEach(delegate(Constraint constraint)
            {
                _sbScript.AppendFormat(System.Globalization.CultureInfo.InvariantCulture, "ALTER TABLE [{0}] ADD CONSTRAINT [{1}] FOREIGN KEY ({2}) REFERENCES [{3}]({4}) ON DELETE {5} ON UPDATE {6};{7}"
                    , constraint.ConstraintTableName
                    , constraint.ConstraintName
                    , constraint.Columns.ToString()
                    , constraint.UniqueConstraintTableName
                    , constraint.UniqueColumns.ToString()
                    , constraint.DeleteRule
                    , constraint.UpdateRule
                    , Environment.NewLine);
                _sbScript.Append(_sep);
            });            
        
        }

        /// <summary>
        /// Generates the foreign keys.
        /// </summary>
        /// <param name="tableName">Name of the table.</param>
        public void GenerateForeignKeys(string tableName)
        {
            List<Constraint> foreingKeys = _repository.GetAllForeignKeys(tableName);

            foreach (Constraint constraint in foreingKeys)
            {
                _sbScript.AppendFormat(System.Globalization.CultureInfo.InvariantCulture, "ALTER TABLE [{0}] ADD CONSTRAINT [{1}] FOREIGN KEY ({2}) REFERENCES [{3}]({4}) ON DELETE {5} ON UPDATE {6};{7}"
                    , constraint.ConstraintTableName
                    , constraint.ConstraintName
                    , constraint.Columns.ToString()
                    , constraint.UniqueConstraintTableName
                    , constraint.UniqueColumns.ToString()
                    , constraint.DeleteRule
                    , constraint.UpdateRule
                    , Environment.NewLine);
                _sbScript.Append(_sep);
            };

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

        /// <summary>
        /// Generates the index script.
        /// </summary>
        /// <param name="tableName">Name of the table.</param>
        /// <param name="indexName">Name of the index.</param>
        public void GenerateIndexScript(string tableName, string indexName)
        {
            GenerateSingleIndex(tableName, indexName);
        }

        /// <summary>
        /// Generates the index drop statement.
        /// </summary>
        /// <param name="tableName">Name of the table.</param>
        /// <param name="indexName">Name of the index.</param>
        public void GenerateIndexDrop(string tableName, string indexName)
        {
            List<Index> tableIndexes = _repository.GetIndexesFromTable(tableName);
            IOrderedEnumerable<Index> indexesByName = from i in tableIndexes
                                                      where i.IndexName == indexName
                                                      orderby i.OrdinalPosition
                                                      select i;
            if (indexesByName.Count() > 0)
            {
                _sbScript.AppendFormat("DROP INDEX [{0}].[{1}];{2}", tableName, indexName, Environment.NewLine);
                _sbScript.Append(_sep);
            }
            else
            {
                _sbScript.AppendFormat("ALTER TABLE [{0}] DROP CONSTRAINT [{1}];{2}", tableName, indexName, Environment.NewLine);
                _sbScript.Append(_sep);                
            }
        }

        public void GenerateIndexOnlyDrop(string tableName, string indexName)
        {
            _sbScript.AppendFormat("DROP INDEX [{0}].[{1}];{2}", tableName, indexName, Environment.NewLine);
            _sbScript.Append(_sep);            
        }

        /// <summary>
        /// Generates the index statistics.
        /// </summary>
        /// <param name="tableName">Name of the table.</param>
        /// <param name="indexName">Name of the index.</param>
        public void GenerateIndexStatistics(string tableName, string indexName)
        {
            _sbScript.AppendFormat("sp_show_statistics '{0}', '{1}';{2}", tableName, indexName, Environment.NewLine);
            _sbScript.Append(_sep);
            _sbScript.AppendFormat("sp_show_statistics_columns '{0}', '{1}';{2}", tableName, indexName, Environment.NewLine);
            _sbScript.Append(_sep);
            _sbScript.AppendFormat("sp_show_statistics_steps '{0}', '{1}';{2}", tableName, indexName, Environment.NewLine);
            _sbScript.Append(_sep);
        }

        /// <summary>
        /// Generates the index create statement.
        /// </summary>
        /// <param name="tableName">Name of the table.</param>
        private void GenerateIndex(string tableName)
        {
            List<Index> tableIndexes = _repository.GetIndexesFromTable(tableName);
            if (tableIndexes.Count > 0)
            {
                IEnumerable<string> uniqueIndexNameList = tableIndexes.Select(i => i.IndexName).Distinct();

                foreach (string uniqueIndexName in uniqueIndexNameList)
                {
                    GenerateSingleIndex(tableName, uniqueIndexName);
                }
            }
        }


        private void GenerateSingleIndex(string tableName, string uniqueIndexName)
        {

            List<Index> tableIndexes = _repository.GetIndexesFromTable(tableName);
            
            IOrderedEnumerable<Index> indexesByName = from i in tableIndexes
                                                      where i.IndexName == uniqueIndexName
                                                      orderby i.OrdinalPosition
                                                      select i;
            if (indexesByName.Count() > 0)
            {
                _sbScript.Append("CREATE ");
                ////
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
            else
            {
                GeneratePrimaryKeys(tableName);
            }
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

        /// <summary>
        /// Generates the table columns.
        /// </summary>
        /// <param name="tableName">Name of the table.</param>
        /// <returns></returns>
        public List<string> GenerateTableColumns(string tableName)
        {
            return  (from a in _allColumns
                    where a.TableName == tableName
                    select a.ColumnName).ToList();
        }

        public void GenerateColumnAddScript(Column column)
        {
            if (column.IsNullable == YesNoOption.NO)
            {
                _sbScript.AppendLine("-- Adding as column with NOT NULL is not allowed, set a default value or allow NULL");
            }
            _sbScript.Append(string.Format("ALTER TABLE [{0}] ADD {1}{2}", column.TableName, GenerateColumLine(false, column, _batchForAzure), Environment.NewLine));
            _sbScript.Append(_sep);
        }

        public void GenerateColumnDropScript(Column column)
        {
            _sbScript.Append(string.Format("ALTER TABLE [{0}] DROP COLUMN [{1}]{2}", column.TableName, column.ColumnName, Environment.NewLine));
            _sbScript.Append(_sep);
        }

        public void GenerateColumnAlterScript(Column column)
        {
            _sbScript.Append(string.Format("ALTER TABLE [{0}] ALTER COLUMN {1}{2}", column.TableName, GenerateColumLine(false, column, _batchForAzure), Environment.NewLine));
            _sbScript.Append(_sep);
        }

        public void GenerateColumnSetDefaultScript(Column column)
        {
            // ALTER TABLE MyCustomers ALTER COLUMN CompanyName SET DEFAULT 'A. Datum Corporation'
            _sbScript.Append(string.Format("ALTER TABLE [{0}] ALTER COLUMN [{1}] SET DEFAULT {2}{3}", column.TableName, column.ColumnName, column.ColumnDefault, Environment.NewLine));
            _sbScript.Append(_sep);
        }

        public void GenerateColumnDropDefaultScript(Column column)
        {
            //ALTER TABLE MyCustomers ALTER COLUMN CompanyName DROP DEFAULT
            _sbScript.Append(string.Format("ALTER TABLE [{0}] ALTER COLUMN [{1}] DROP DEFAULT{2}", column.TableName, column.ColumnName, Environment.NewLine));
            _sbScript.Append(_sep);
        }


        public void GeneratePrimaryKeyDrop(PrimaryKey primaryKey, string tableName)
        {
            //ALTER TABLE xx DROP CONSTRAINT yy
            _sbScript.Append(string.Format("ALTER TABLE [{0}] DROP CONSTRAINT [{1}]{2}", tableName, primaryKey.KeyName, Environment.NewLine));
            _sbScript.Append(_sep);
        }

        public void GenerateForeignKey(Constraint constraint)
        {
            _sbScript.AppendFormat(System.Globalization.CultureInfo.InvariantCulture, "ALTER TABLE [{0}] ADD CONSTRAINT [{1}] FOREIGN KEY ({2}) REFERENCES [{3}]({4}) ON DELETE {5} ON UPDATE {6};{7}"
                , constraint.ConstraintTableName
                , constraint.ConstraintName
                , constraint.Columns.ToString()
                , constraint.UniqueConstraintTableName
                , constraint.UniqueColumns.ToString()
                , constraint.DeleteRule
                , constraint.UpdateRule
                , Environment.NewLine);
            _sbScript.Append(_sep);
        }

        public void GenerateForeignKey(string tableName, string keyName)
        {
            var key = _allForeignKeys.Where(c => c.ConstraintTableName == tableName && c.ConstraintName == keyName).SingleOrDefault();
            if (key != null)
            {
                GenerateForeignKey(key);
            }
        }


        public void GenerateForeignKeyDrop(Constraint constraint)
        {
            _sbScript.Append(string.Format("ALTER TABLE [{0}] DROP CONSTRAINT [{1}]{2}", constraint.ConstraintTableName, constraint.ConstraintName, Environment.NewLine));
            _sbScript.Append(_sep);
        }

        public void GenerateForeignKeyDrop(string tableName, string keyName)
        {
            var key = _allForeignKeys.Where(c => c.ConstraintTableName == tableName && c.ConstraintName == keyName).SingleOrDefault();
            if (key != null)
            {
                GenerateForeignKeyDrop(key);
            }
        }

        internal void GenerateAllAndSave(bool includeData, bool saveImages)
        {
            GenerateTable(includeData);
            if (_batchForAzure)
            {
                GeneratePrimaryKeys();
            }
            if (includeData)
            {
                GenerateTableContent(saveImages);
            }
            if (!_batchForAzure)
            {
                GeneratePrimaryKeys();
            }
            GenerateIndex();
            GenerateForeignKeys();
            Helper.WriteIntoFile(GeneratedScript, _outFile, this.FileCounter);
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

        private void Init(IRepository repository, string outFile)
        {
            _outFile = outFile;
            _repository = repository;
            _sbScript = new StringBuilder(10485760);
            _tableNames = _repository.GetAllTableNames();
            _allColumns = _repository.GetColumnsFromTable();
            _allForeignKeys = repository.GetAllForeignKeys();
            _allPrimaryKeys = repository.GetAllPrimaryKeys();

            if (_repository.IsServer())
            {
                // Check if datatypes are supported when exporting from server
                // Either they can be converted, are supported, or an exception is thrown (if not supported)
                // Currently only sql_variant is not supported
                foreach (Column col in _allColumns)
                {
                    col.CharacterMaxLength = Helper.CheckDateColumnLength(col.DataType, col);
                    col.DateFormat = Helper.CheckDateFormat(col.DataType);

                    // Check if the current column points to a unique identity column,
                    // as the two columns' datatypes must match
                    bool refToIdentity = false;
                    Dictionary<string, Constraint> columnForeignKeys = new Dictionary<string, Constraint>();

                    // Fix for multiple constraints with same columns
                    var _tableKeys = _allForeignKeys.Where(c => c.ConstraintTableName == col.TableName);
                    foreach (var constraint in _tableKeys)
                    { 
                        if (!columnForeignKeys.ContainsKey(constraint.Columns.ToString()))
                        {
                            columnForeignKeys.Add(constraint.Columns.ToString(), constraint);
                        }
                    }

                    if (columnForeignKeys.ContainsKey(string.Format("[{0}]", col.ColumnName)))
                    {
                        var refCol = _allColumns.Where(c => c.TableName == columnForeignKeys[string.Format("[{0}]", col.ColumnName)].UniqueConstraintTableName
                            && string.Format("[{0}]", c.ColumnName) == columnForeignKeys[string.Format("[{0}]", col.ColumnName)].UniqueColumnName).Single();
                        if (refCol != null && refCol.AutoIncrementBy > 0)
                        {
                            refToIdentity = true;
                        }
                    }

                    // This modifies the datatype to be SQL Compact compatible
                    col.DataType = Helper.CheckDataType(col.DataType, col, refToIdentity);
                }
                _sbScript.AppendFormat("-- Script Date: {0} {1}  - Generated by Export2SqlCe version {2}", DateTime.Now.ToShortDateString(), DateTime.Now.ToShortTimeString(), System.Reflection.Assembly.GetExecutingAssembly().GetName().Version);
            }
            else
            {
                _sbScript.AppendFormat("-- Script Date: {0} {1}  - Generated by ExportSqlCe version {2}", DateTime.Now.ToShortDateString(), DateTime.Now.ToShortTimeString(), System.Reflection.Assembly.GetExecutingAssembly().GetName().Version);
            }
            _sbScript.AppendLine();
            if (!string.IsNullOrEmpty(_outFile) && !_repository.IsServer())
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

        internal void GenerateTable(bool includeData)
        {
            foreach (string tableName in _tableNames)
                GenerateTableCreate(tableName, includeData);

        }

        public void GenerateTableCreate(string tableName)
        {
            GenerateTableCreate(tableName, false);
        }

        internal void GenerateTableCreate(string tableName, bool includeData)
        {
            List<Column> columns = _allColumns.Where(c => c.TableName == tableName).ToList();
            if (columns.Count > 0)
            {
                _sbScript.AppendFormat("CREATE TABLE [{0}] ({1}  ", tableName, Environment.NewLine);

                foreach (Column col in columns)
                {
                    string line = GenerateColumLine(includeData, col, _batchForAzure);
                    _sbScript.AppendFormat("{0}{1}, ", line.Trim(), Environment.NewLine);
                }

                // Remove the last comma
                _sbScript.Remove(_sbScript.Length - 2, 2);
                _sbScript.AppendFormat(");{0}", Environment.NewLine);
                _sbScript.Append(_sep);
            }
        }

        private static string GenerateColumLine(bool includeData, Column col, bool azure)
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
                    string rowGuidCol = string.Empty;
                    if (col.RowGuidCol && !azure)
                    {
                        rowGuidCol = "ROWGUIDCOL";
                    }
                    if (includeData)
                    {
                        line = string.Format(System.Globalization.CultureInfo.InvariantCulture,
                            "[{0}] {1} {2} {3} {4}{5}"
                            , col.ColumnName
                            , col.DataType
                            , (col.IsNullable == YesNoOption.YES ? "NULL" : "NOT NULL")
                            , (col.ColumnHasDefault ? "DEFAULT " + col.ColumnDefault : string.Empty)
                            , rowGuidCol
                            , (col.AutoIncrementBy > 0 ? string.Format(System.Globalization.CultureInfo.InvariantCulture, "IDENTITY ({0},{1})", col.AutoIncrementNext, col.AutoIncrementBy) : string.Empty)
                            );
                    }
                    else
                    {
                        line = string.Format(System.Globalization.CultureInfo.InvariantCulture,
                            "[{0}] {1} {2} {3} {4}{5}"
                            , col.ColumnName
                            , col.DataType
                            , (col.IsNullable == YesNoOption.YES ? "NULL" : "NOT NULL")
                            , (col.ColumnHasDefault ? "DEFAULT " + col.ColumnDefault : string.Empty)
                            , rowGuidCol
                            , (col.AutoIncrementBy > 0 ? string.Format(System.Globalization.CultureInfo.InvariantCulture, "IDENTITY ({0},{1})", col.AutoIncrementSeed, col.AutoIncrementBy) : string.Empty)
                            );
                    }

                    break;
            }
            return line;
        }

        internal void GenerateTableContent(bool saveImageFiles)
        {
            foreach (string tableName in _tableNames)
            {
                GenerateTableContent(tableName, saveImageFiles);
            }
        }


    }
}
