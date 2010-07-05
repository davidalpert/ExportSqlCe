using System;
using System.Collections.Generic;
using System.Data;

namespace ErikEJ.SqlCeScripting
{
    public interface IRepository : IDisposable
    {
        List<string> GetAllTableNames();
        List<Column> GetColumnsFromTable();
        DataTable GetDataFromTable(string tableName, List<Column> columns);
        IDataReader GetDataFromReader(string tableName);
        List<PrimaryKey> GetPrimaryKeysFromTable(string tableName);
        List<Constraint> GetAllForeignKeys();
        List<Constraint> GetAllForeignKeys(string tableName);
        List<Index> GetIndexesFromTable(string tableName);
        List<KeyValuePair<string, string>> GetDatabaseInfo();
        Boolean HasIdentityColumn(string tableName);
        Boolean IsServer();
        Int32 GetRowVersionOrdinal(string tableName);
        Int64 GetRowCount(string tableName);
        void RenameTable(string oldName, string newName);
        DataSet ExecuteSql(string script);
        DataSet ParseSql(string script);
        /// <summary>
        /// Get the local Datetime for last sync
        /// </summary>
        /// <param name="publication"> Publication id: EEJx:Northwind:NwPubl</param>
        /// <returns></returns>
        DateTime GetLastSuccessfulSyncTime(string publication);
        /// <summary>
        /// Returns a list of all Merge subscriptions in the database
        /// </summary>
        /// <returns></returns>
        List<string> GetAllSubscriptionNames();
        
    }


}
