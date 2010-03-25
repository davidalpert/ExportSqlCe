using System;
using System.Collections.Generic;
using System.Data;

namespace ExportSqlCE
{
    interface IRepository : IDisposable
    {
        List<string> GetAllTableNames();
        List<Column> GetColumnsFromTable();
        DataTable GetDataFromTable(string tableName, List<Column> columns);
        IDataReader GetDataFromReader(string tableName);
        List<string> GetPrimaryKeysFromTable(string tableName);
        List<Constraint> GetAllForeignKeys();
        List<Constraint> GetAllForeignKeys(string tableName);
        List<Index> GetIndexesFromTable(string tableName);
        List<KeyValuePair<string, string>> GetDatabaseInfo();
        Boolean HasIdentityColumn(string tableName);
        Boolean IsServer();
        Int32 GetRowVersionOrdinal(string tableName);
        Int64 GetRowCount(string tableName);
        void RenameTable(string oldName, string newName);
        DataSet GetSchemaDataSet(List<string> tableNames);
    }


}
