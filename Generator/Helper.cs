using System.Text;
using System.IO;
using System.Collections.Generic;
using System.Linq;
using System;

namespace ErikEJ.SqlCeScripting
{
    public static class Helper
    {

        public static string FinalFiles
        {
            get
            {
                if (!string.IsNullOrEmpty(finalFiles))
                {
                    if (finalFiles.EndsWith(", ", System.StringComparison.OrdinalIgnoreCase))
                    {
                        finalFiles = finalFiles.Remove(finalFiles.Length - 2);
                    }
                }
                return finalFiles;
            }
            set
            {
                finalFiles = value;
            }
        }

        private static string finalFiles;

        public static void WriteIntoFile(string script, string fileLocation, int increment)
        {
            if (increment > -1)
            {
                if (!finalFiles.Contains(","))
                {
                    finalFiles = string.Empty;
                }
                string ext = Path.GetExtension(System.IO.Path.GetFileName(fileLocation));
                string path = Path.GetDirectoryName(fileLocation);
                string name = Path.GetFileNameWithoutExtension(fileLocation);
                fileLocation = Path.Combine(path, name + "_" + increment.ToString(System.Globalization.CultureInfo.InvariantCulture));
                if (!string.IsNullOrEmpty(ext))
                {
                    fileLocation = fileLocation + ext;
                    finalFiles = finalFiles + fileLocation + ", ";
                }
            }
            using (FileStream fs = new FileStream(fileLocation, FileMode.Create, FileAccess.Write, FileShare.Read))
            {
                using (StreamWriter sw = new StreamWriter(fs, Encoding.Unicode))
                {
                    sw.WriteLine(script);
                    sw.Flush();
                }
            }
        }

        public static string FixConnectionString(string connectionString, int timeout)
        {
            return connectionString.Replace(string.Format(System.Globalization.CultureInfo.InvariantCulture, ";Timeout = \"{0}\"", timeout), string.Empty);
        }


        public static string ScriptDatabaseToFile(string fileName, Scope scope, IRepository repository)
        {
            Helper.FinalFiles = fileName;
            System.Diagnostics.Stopwatch sw = new System.Diagnostics.Stopwatch();
            sw.Start();
            var generator = new Generator(repository, fileName);
            switch (scope)
            {
                case Scope.Schema:
                    generator.GenerateAllAndSave(false, false);
                    break;
                case Scope.SchemaData:
                    generator.GenerateAllAndSave(true, false);
                    break;
                case Scope.SchemaDataBlobs:
                    generator.GenerateAllAndSave(true, true);
                    break;
                default:
                    break;
            }
            sw.Stop();
            return string.Format(System.Globalization.CultureInfo.InvariantCulture, "Sent script to output file(s) : {0} in {1} ms", Helper.FinalFiles, (sw.ElapsedMilliseconds).ToString(System.Globalization.CultureInfo.CurrentCulture));
        }

        internal static string CheckDataType(string dataType, Column col, bool refToIdentity)
        {
            switch (dataType)
            {
                // These datatypes are supported by SQL Compact 3.5
                //Fallthrough
                case "bigint":
                case "binary":
                case "bit":
                case "datetime":
                case "float":
                case "image":
                case "int":
                case "money":
                case "nchar":
                case "ntext":
                case "real":
                case "rowversion":
                case "uniqueidentifier":
                    return dataType;

                // Conditional conversion
                case "smallint":
                case "tinyint":
                    // Only int or bigint allowed as IDENTITY
                    if (col.AutoIncrementBy > 0)
                    {
                        return "int";
                    }
                    if (refToIdentity)
                    {
                        return "int";
                    }
                    return dataType;

                case "numeric":
                    // Only int or bigint allowed as IDENTITY
                    if (col.AutoIncrementBy > 0)
                    {
                        return "bigint";
                    }
                    if (refToIdentity)
                    {
                        return "bigint";
                    }
                    return dataType;

                case "nvarchar":
                    // Support for nvarchar(MAX)
                    if (col.CharacterMaxLength == -1)
                    {
                        return "ntext";
                    };
                    return dataType;

                case "varbinary":
                    // Support for varbinary(MAX)
                    if (col.CharacterMaxLength == -1)
                    {
                        return "image";
                    };
                    return dataType;

                //These datatypes are converted
                // See also http://msdn.microsoft.com/en-us/library/ms143241.aspx
                case "char":
                    return "nchar";
                case "varchar":
                    // Support for varchar(MAX)
                    if (col.CharacterMaxLength == -1)
                    {
                        return "ntext";
                    };
                    return "nvarchar";
                case "text":
                    return "ntext";
                case "timestamp":
                    return "rowversion";
                case "decimal":
                    return "numeric";
                case "smalldatetime":
                    return "datetime";
                case "smallmoney":
                    return "money";
                case "xml":
                    return "ntext";
                case "geography":
                    return "image";
                case "geometry":
                    return "image";
                case "hierarchyid":
                    return "image";

                //Fallthrough
                case "time":
                case "datetime2":
                case "datetimeoffset":
                case "date":
                    return "nvarchar";

                default:
                    // Currently not supported: sql_variant
                    throw new System.NotSupportedException(string.Format(System.Globalization.CultureInfo.InvariantCulture, "Data type {0} in table {1}, colum {2} is not supported, please change to a supported type", dataType, col.TableName, col.ColumnName));
            }

        }

        internal static int CheckDateColumnLength(string dataType, Column col)
        {
            // See also http://msdn.microsoft.com/en-us/library/ms143241.aspx
            switch (dataType)
            {
                case "time":
                    return 16;
                case "datetime2":
                    return 27;
                case "datetimeoffset":
                    return 34;
                case "date":
                    return 10;
                default:
                    return col.CharacterMaxLength;
            }
        }

        internal static DateFormat CheckDateFormat(string dataType)
        {
            // See also http://msdn.microsoft.com/en-us/library/ms143241.aspx
            switch (dataType)
            {
                case "datetime2":
                    return DateFormat.DateTime2;
                case "date":
                    return DateFormat.Date;
                case "datetime":
                    return DateFormat.DateTime;
                default:
                    return DateFormat.None;
            }
        }

        // Contrib from hugo on CodePlex - thanks!
        internal static List<Constraint> GetGroupForeingKeys(List<Constraint> foreignKeys)
        {
            var groupedForeingKeys = new List<Constraint>();

            var uniqueConstaints = (from c in foreignKeys
                                    select c.ConstraintName).Distinct();

            foreach (string item in uniqueConstaints)
            {
                string value = item;
                var constraints = foreignKeys.Where(c => c.ConstraintName.Equals(value, System.StringComparison.Ordinal)).ToList();

                if (constraints.Count == 1)
                {
                    Constraint constraint = constraints[0];
                    constraint.Columns.Add(constraint.ColumnName);
                    constraint.UniqueColumns.Add(constraint.UniqueColumnName);
                    groupedForeingKeys.Add(constraint);
                }
                else
                {
                    var newConstraint = new Constraint { ConstraintTableName = constraints[0].ConstraintTableName, ConstraintName = constraints[0].ConstraintName, UniqueConstraintTableName = constraints[0].UniqueConstraintTableName, UniqueConstraintName = constraints[0].UniqueConstraintName, DeleteRule = constraints[0].DeleteRule, UpdateRule = constraints[0].UpdateRule, Columns = new ColumnList(), UniqueColumns = new ColumnList() };
                    foreach (Constraint c in constraints)
                    {
                        newConstraint.Columns.Add(c.ColumnName);
                        newConstraint.UniqueColumns.Add(c.UniqueColumnName);
                    }
                    groupedForeingKeys.Add(newConstraint);
                }
            }
            return groupedForeingKeys;
        }

        public static string ShowErrors(System.Data.SqlClient.SqlException e)
        {
            System.Data.SqlClient.SqlErrorCollection errorCollection = e.Errors;

            StringBuilder bld = new StringBuilder();
            Exception inner = e.InnerException;

            if (null != inner)
            {
                bld.Append("Inner Exception: " + inner.ToString());
            }
            // Enumerate the errors to a message box.
            foreach (System.Data.SqlClient.SqlError err in errorCollection)
            {
                bld.Append("\n Message   : " + err.Message);
                bld.Append("\n Source    : " + err.Source);
                bld.Append("\n Number    : " + err.Number);

            }
            return bld.ToString();
        }

        public static string ShowErrors(System.Data.SqlServerCe.SqlCeException e)
        {
            System.Data.SqlServerCe.SqlCeErrorCollection errorCollection = e.Errors;

            StringBuilder bld = new StringBuilder();
            Exception inner = e.InnerException;

            if (!string.IsNullOrEmpty(e.HelpLink))
            {
                bld.Append("\nCommand text: ");
                bld.Append(e.HelpLink);
            }

            if (null != inner)
            {
                bld.Append("\nInner Exception: " + inner.ToString());
            }
            // Enumerate the errors to a message box.
            foreach (System.Data.SqlServerCe.SqlCeError err in errorCollection)
            {
                bld.Append("\n Error Code: " + err.HResult.ToString("X", System.Globalization.CultureInfo.InvariantCulture));
                bld.Append("\n Message   : " + err.Message);
                bld.Append("\n Minor Err.: " + err.NativeError);
                bld.Append("\n Source    : " + err.Source);

                // Enumerate each numeric parameter for the error.
                foreach (int numPar in err.NumericErrorParameters)
                {
                    if (0 != numPar) bld.Append("\n Num. Par. : " + numPar);
                }

                // Enumerate each string parameter for the error.
                foreach (string errPar in err.ErrorParameters)
                {
                    if (!string.IsNullOrEmpty(errPar)) bld.Append("\n Err. Par. : " + errPar);
                }
            }
            return bld.ToString();
        }

    }

#if V31
#else
    public static class SqlCeUpgrade
    {
        public static void EnsureVersion40(this System.Data.SqlServerCe.SqlCeEngine engine, string filename)
        {
            SQLCEVersion fileversion = DetermineVersion(filename);
            if (fileversion == SQLCEVersion.SQLCE20)
                throw new ApplicationException("Unable to upgrade from 2.0 to 4.0");

            if (SQLCEVersion.SQLCE40 > fileversion)
            {
                engine.Upgrade();
            }
        }
        private enum SQLCEVersion
        {
            SQLCE20 = 0,
            SQLCE30 = 1,
            SQLCE35 = 2,
            SQLCE40 = 3
        }
        private static SQLCEVersion DetermineVersion(string filename)
        {
            var versionDictionary = new Dictionary<int, SQLCEVersion> 
            { 
                { 0x73616261, SQLCEVersion.SQLCE20 }, 
                { 0x002dd714, SQLCEVersion.SQLCE30},
                { 0x00357b9d, SQLCEVersion.SQLCE35},
                { 0x003d0900, SQLCEVersion.SQLCE40}
            };
            int versionLONGWORD = 0;
            try
            {
                using (var fs = new FileStream(filename, FileMode.Open))
                {
                    fs.Seek(16, SeekOrigin.Begin);
                    using (BinaryReader reader = new BinaryReader(fs))
                    {
                        versionLONGWORD = reader.ReadInt32();
                    }
                }
            }
            catch
            {
                throw;
            }
            if (versionDictionary.ContainsKey(versionLONGWORD))
            {
                return versionDictionary[versionLONGWORD];
            }
            else
            {
                throw new ApplicationException("Unable to determine database file version");
            }
        }


    }
#endif
}
