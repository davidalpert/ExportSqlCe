using System.Text;
using System.IO;

namespace ExportSqlCE
{
    internal static class Helper
    {

        internal static string FinalFiles 
        {
            get
            {
                if (!string.IsNullOrEmpty(finalFiles))
                {
                    if (finalFiles.EndsWith(", "))
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

        internal static void WriteIntoFile(string script, string fileLocation, int increment)
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

        internal static string FixConnectionString(string connectionString, int timeout)
        {
            return connectionString.Replace(string.Format(";Timeout = \"{0}\"", timeout), string.Empty);
        }
        

        internal static string ScriptDatabaseToFile(string fileName, Scope scope, IRepository repository)
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
            return string.Format("Sent script to output file(s) : {0} in {1} ms", Helper.FinalFiles, (sw.ElapsedMilliseconds).ToString());
        }

        internal static string CheckDataType(string dataType, Column col)
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
                case "numeric":
                case "real":
                case "rowversion":
                case "uniqueidentifier":
                    return dataType;

                // Conditional conversion
                case "smallint":
                    // Only int or bigint allowed as IDENTITY
                    if (col.AutoIncrementBy > 0)
                    {
                        return "int";
                    }
                    return dataType;

                case "tinyint":
                    // Only int or bigint allowed as IDENTITY
                    if (col.AutoIncrementBy > 0)
                    {
                        return "int";
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
                    throw new System.Exception(string.Format("Data type {0} in table {1}, colum {2} is not supported, please change to a supported type", dataType, col.TableName, col.ColumnName));
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



    }
}
