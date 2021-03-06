﻿using System.Text;
using System.IO;
using System.Collections.Generic;
using System.Linq;
using System;
using System.Globalization;

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

        public static void WriteIntoFile(string script, string fileLocation, int increment, bool sqlite)
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
            FileStream fs = null;
            StreamWriter sw = null;
            try
            {
                fs = new FileStream(fileLocation, FileMode.Create, FileAccess.Write, FileShare.Read);
                System.Text.Encoding encoder = new System.Text.UTF8Encoding(false);
                if (!sqlite)
                    encoder = new System.Text.UnicodeEncoding();
                
                sw = new StreamWriter(fs, encoder);
                fs = null;
                sw.WriteLine(script);
                sw.Flush();
            }
            finally
            {
                if (sw != null)
                    sw.Close();
                if (fs != null)
                    fs.Close();
            }
        }

        public static string FixConnectionString(string connectionString, int timeout)
        {
            return connectionString.Replace(string.Format(System.Globalization.CultureInfo.InvariantCulture, ";Timeout = \"{0}\"", timeout), string.Empty);
        }

        internal static string CheckDataType(string dataType, Column col, bool refToIdentity, bool preserveDateAndDateTime2)
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
                    //SQL Server allows up to 8000 chars, SQL Compact only 4000
                    if (col.CharacterMaxLength > 4000)
                    {
                        return "ntext";
                    };
                    return "nchar";
                case "varchar":
                    // Support for varchar(MAX)
                    //SQL Server allows up to 8000 chars, SQL Compact only 4000
                    if (col.CharacterMaxLength == -1 || col.CharacterMaxLength > 4000)
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

                // Work item 17681         
                case "datetime2":
                case "date":
                    if (preserveDateAndDateTime2)
                    {
                        return "nvarchar";
                    }
                    else
                    {
                        return "datetime";
                    }
                //Fallthrough
                case "time":
                case "datetimeoffset":
                    return "nvarchar";

                default:
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
        internal static List<Constraint> GetGroupForeingKeys(List<Constraint> foreignKeys, List<string> allTables)
        {
            var groupedForeingKeys = new List<Constraint>();

            var uniqueTables = (from c in foreignKeys
                                select c.ConstraintTableName).Distinct().ToList();
            int i = 1;
            foreach (string tableName in uniqueTables)
            {
                {
                    var uniqueConstraints = (from c in foreignKeys
                                             where c.ConstraintTableName == tableName
                                            select c.ConstraintName).Distinct().ToList();
                    foreach (string item in uniqueConstraints)
                    {
                        string value = item;
                        var constraints = foreignKeys.Where(c => c.ConstraintName.Equals(value, System.StringComparison.Ordinal) && c.ConstraintTableName == tableName).ToList();

                        if (constraints.Count == 1)
                        {
                            Constraint constraint = constraints[0];
                            constraint.Columns.Add(constraint.ColumnName);
                            constraint.UniqueColumns.Add(constraint.UniqueColumnName);
                            var found = groupedForeingKeys.Where(fk => fk.ConstraintName == constraint.ConstraintName && fk.ConstraintTableName != constraint.ConstraintTableName).Any();
                            if (found)
                            {
                                constraint.ConstraintName = constraint.ConstraintName + i.ToString();
                                i++;
                            }
                            else
                            {
                                var tfound = allTables.Where(ut => ut == constraint.ConstraintName).Any();
                                if (tfound)
                                {
                                    constraint.ConstraintName = constraint.ConstraintName + i.ToString();
                                    i++;
                                }
                            }

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
                            var found = groupedForeingKeys.Where(fk => fk.ConstraintName == newConstraint.ConstraintName && fk.ConstraintTableName != newConstraint.ConstraintTableName).Any();
                            if (found)
                            {
                                newConstraint.ConstraintName = newConstraint.ConstraintName + i.ToString();
                                i++;
                            }
                            groupedForeingKeys.Add(newConstraint);
                        }
                    }
                }
            }
            return groupedForeingKeys;
        }

        internal static List<PrimaryKey> EnsureUniqueNames(List<PrimaryKey> primaryKeys)
        {

            // Fix for duplicate constraint names (which causes script failure in SQL Server)
            // https://connect.microsoft.com/SQLServer/feedback/details/586600/duplicate-constraint-foreign-key-name
            var fixedPrimaryKeys = new List<PrimaryKey>();

            var uniqueTables = (from c in primaryKeys
                                select c.TableName).Distinct();
            int i = 1;
            foreach (string tableName in uniqueTables)
            {
                {
                    var uniqueKeys = (from c in primaryKeys
                                             where c.TableName == tableName
                                             select c.KeyName).Distinct();
                    foreach (string value in uniqueKeys)
                    {
                        var pks = primaryKeys.Where(c => c.KeyName.Equals(value, System.StringComparison.Ordinal) && c.TableName == tableName).ToList();
                        if (pks.Count > 0)
                        {
                            var found = primaryKeys.Where(fk => fk.KeyName == pks[0].KeyName && fk.TableName != pks[0].TableName).Any();
                            string newKeyName = pks[0].KeyName;
                            if (found)
                            {
                                newKeyName = pks[0].KeyName + i.ToString();
                                i++;
                            }
                            foreach (var item in pks)
                            {
                                PrimaryKey pk = new PrimaryKey();
                                pk.ColumnName = item.ColumnName;
                                pk.TableName = item.TableName;
                                pk.KeyName = newKeyName;
                                fixedPrimaryKeys.Add(pk);
                            }

                        }                        
                    }
                }
            }
            return fixedPrimaryKeys;
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
            foreach (System.Data.SqlClient.SqlError err in errorCollection)
            {
                bld.AppendLine("Message   : " + err.Message);
                bld.AppendLine("Level     : " + err.Class);
                bld.AppendLine("State     : " + err.State);
                bld.AppendLine("Procedure : " + err.Procedure);
                bld.AppendLine("Line      : " + err.LineNumber);
                bld.AppendLine("Source    : " + err.Source);
                bld.AppendLine("Number    : " + err.Number);                
                bld.AppendLine("Server    : " + err.Server);
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

        // Returns the human-readable file size for an arbitrary, 64-bit file size
        //  The default format is "0.### XB", e.g. "4.2 KB" or "1.434 GB"
        // http://www.somacon.com/p576.php
        public static string GetSizeReadable(long i)
        {
            string sign = (i < 0 ? "-" : "");
            double readable = (i < 0 ? -i : i);
            string suffix;
            if (i >= 0x1000000000000000) // Exabyte
            {
                suffix = "EB";
                readable = (double)(i >> 50);
            }
            else if (i >= 0x4000000000000) // Petabyte
            {
                suffix = "PB";
                readable = (double)(i >> 40);
            }
            else if (i >= 0x10000000000) // Terabyte
            {
                suffix = "TB";
                readable = (double)(i >> 30);
            }
            else if (i >= 0x40000000) // Gigabyte
            {
                suffix = "GB";
                readable = (double)(i >> 20);
            }
            else if (i >= 0x100000) // Megabyte
            {
                suffix = "MB";
                readable = (double)(i >> 10);
            }
            else if (i >= 0x400) // Kilobyte
            {
                suffix = "KB";
                readable = (double)i;
            }
            else
            {
                return i.ToString(sign + "0 B"); // Byte
            }
            readable = readable / 1024;
            
            NumberFormatInfo nfi = (NumberFormatInfo)
            CultureInfo.InvariantCulture.NumberFormat.Clone();
            nfi.NumberGroupSeparator = "";

            return sign + readable.ToString("0.### ") + suffix;
        }

        public static IRepository CreateRepository(string connectionString)
        {
            //if your Data source connection string argument does not specify the User Id, Initial catalog, Integrated security, 
            //or Trusted Connection arguments, and instead specifies a file path, 
            //the source will be treated as a SQL Server Compact database
            bool isServer = false;
            connectionString = connectionString.Replace(" =", "=");
            connectionString = connectionString.Replace("= ", "=");
            if (connectionString.ToUpperInvariant().Contains("USER ID="))
                isServer = true;
            if (connectionString.ToUpperInvariant().Contains("UID="))
                isServer = true;
            if (connectionString.ToUpperInvariant().Contains("INITIAL CATALOG="))
                isServer = true;
            if (connectionString.ToUpperInvariant().Contains("INTEGRATED SECURITY="))
                isServer = true;
            if (connectionString.ToUpperInvariant().Contains("TRUSTED_CONNECTION="))
                isServer = true;
            if (isServer)
            {
#if V40
                return new ServerDBRepository4(connectionString);
#else
                return new ServerDBRepository(connectionString);
#endif

            }
            else
            {
#if V40
                return new DB4Repository(connectionString);
#else
#if SERVER
                return new ServerDBRepository(connectionString);
#else
                return new DBRepository(connectionString);
#endif
#endif
            }
        }


        public static IGenerator CreateGenerator(IRepository repository)
        {
#if V40
                return new Generator4(repository);
#else
                return new Generator(repository);
#endif
        }

        public static IGenerator CreateGenerator(IRepository repository, string outFile)
        {
#if V40
                return new Generator4(repository, outFile);
#else
            return new Generator(repository, outFile);
#endif
        }

    }

}




