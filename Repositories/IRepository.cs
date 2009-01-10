using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Text;

namespace ExportSQLCE
{
    interface IRepository
    {
        List<string> GetAllTableNames();
        List<Column> GetColumnsFromTable(string tableName);
        DataTable GetDataFromTable(string tableName);
        List<string> GetPrimaryKeysFromTable(string tableName);
        List<Constraint> GetAllForeignKeys();
        List<Index> GetIndexesFromTable(string tableName);
        List<KeyValuePair<string, string>> GetDatabaseInfo();
        Boolean HasIdentityColumn(string tableName);
        Int32 GetRowVersionOrdinal(string tableName);
    }


}
