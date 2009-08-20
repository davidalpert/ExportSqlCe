using System;
using System.Collections.Generic;
using System.Data;

namespace ExportSqlCE
{
    interface IRepository : IDisposable
    {
        List<string> GetAllTableNames();
        List<Column> GetColumnsFromTable();
        DataTable GetDataFromTable(string tableName);
        IDataReader GetDataFromReader(string tableName);
        List<string> GetPrimaryKeysFromTable(string tableName);
        List<Constraint> GetAllForeignKeys();
        List<Constraint> GetAllForeignKeys(string tableName);
        List<Index> GetIndexesFromTable(string tableName);
        List<KeyValuePair<string, string>> GetDatabaseInfo();
        Boolean HasIdentityColumn(string tableName);
        Int32 GetRowVersionOrdinal(string tableName);
        Int64 GetRowCount(string tableName);
        void RenameTable(string oldName, string newName);
    }


}
