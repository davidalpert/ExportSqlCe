﻿using System;
using System.Collections.Generic;
using System.Data;

namespace ErikEJ.SqlCeScripting
{
    public interface IRepository : IDisposable
    {
        List<string> GetAllTableNames();
        List<string> GetAllTableNamesForExclusion();
        List<Column> GetAllColumns();
        DataTable GetDataFromTable(string tableName, List<Column> columns);
        IDataReader GetDataFromReader(string tableName, List<Column> columns);
        //IDataReader GetDataFromReader(string tableName);
        List<PrimaryKey> GetAllPrimaryKeys();
        List<Constraint> GetAllForeignKeys();
        List<Constraint> GetAllForeignKeys(string tableName);
        List<Index> GetIndexesFromTable(string tableName);
        List<Index> GetAllIndexes();
        List<KeyValuePair<string, string>> GetDatabaseInfo();
        Boolean HasIdentityColumn(string tableName);
        Boolean IsServer();
        Int32 GetRowVersionOrdinal(string tableName);
        Int32 GetIdentityOrdinal(string tableName);
        Int64 GetRowCount(string tableName);
        void RenameTable(string oldName, string newName);
        
        /// <summary>
        /// Runs the supplied script
        /// </summary>
        /// <param name="script"></param>
        /// <returns></returns>
        DataSet ExecuteSql(string script);

        /// <summary>
        /// Runs the supplied script file
        /// </summary>
        /// <param name="script"></param>
        /// <returns></returns>
        void ExecuteSqlFile(string scriptPath);

        /// <summary>
        /// Execute the supplied script, and return the Actual Execution Plan
        /// </summary>
        /// <param name="script"></param>
        /// <param name="showPlanString"></param>
        /// <returns></returns>
        DataSet ExecuteSql(string script, out string showPlanString);

        /// <summary>
        /// Execute the supplied script, and detect if the schema has changed
        /// </summary>
        /// <param name="script"></param>
        /// <param name="schemaChanged"></param>
        /// <returns></returns>
        DataSet ExecuteSql(string script, out bool schemaChanged);

        /// <summary>
        /// Execute the supplied script, and return the Actual Execution Plan, and detect if the schema has changed
        /// </summary>
        /// <param name="script"></param>
        /// <param name="showPlanString"></param>
        /// <param name="schemaChanged"></param>
        /// <returns></returns>
        DataSet ExecuteSql(string script, out string showPlanString, out bool schemaChanged);

        /// <summary>
        /// Get the Showplan XML from a SQL statement
        /// </summary>
        /// <param name="script"></param>
        /// <returns></returns>
        string ParseSql(string script);
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

        /// <summary>
        /// Gets a DataSet with schema information
        /// </summary>
        /// <param name="tables"></param>
        /// <returns></returns>
        DataSet GetSchemaDataSet(List<string> tables);
        
    }


}
