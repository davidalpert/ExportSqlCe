﻿using System.Text;
using System.IO;

namespace ExportSqlCE
{
    internal static class Helper
    {
        internal static void WriteIntoFile(string script, string fileLocation, int increment)
        {
            if (increment > -1)
            {
                string ext = Path.GetExtension(System.IO.Path.GetFileName(fileLocation));
                string path = Path.GetDirectoryName(fileLocation);
                string name = Path.GetFileNameWithoutExtension(fileLocation);
                fileLocation = Path.Combine(path, name + "_" + increment.ToString(System.Globalization.CultureInfo.InvariantCulture));
                if (!string.IsNullOrEmpty(ext))
                {
                    fileLocation = fileLocation + ext;
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

        internal static string CheckDataType(string dataType, Column col)
        {
            switch (dataType)
            {
                // These datatypes are supported by SQL Compact 3.5
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

                default:
                    // Not supported: date, datetime2, datetimeoffset, geography, geometry, hierarchyid, sql_variant, time, xml
                    throw new System.Exception(string.Format("Data type {0} in table {1}, colum {2} is not supported, please change to a supported type", dataType, col.TableName, col.ColumnName));
            }


        }

    }
}
