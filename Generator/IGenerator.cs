using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace ErikEJ.SqlCeScripting
{
    public interface IGenerator
    {
        string ScriptDatabaseToFile(Scope scope);
        void GenerateTableScript(string tableName);
        string GenerateTableData(string tableName, bool saveImageFiles);
        void GenerateTableContent(string tableName, bool saveImageFiles);
        string GeneratedScript {get;}
        void GenerateTableSelect(string tableName);
        void GenerateTableInsert(string tableName);
        void GenerateTableUpdate(string tableName);
        void GenerateTableDelete(string tableName);
        void GenerateTableDrop(string tableName);
        void GenerateTableInsert(string tableName, IList<string> fields, IList<string> values);
        bool ValidColumns(string tableName, IList<string> columns);
        void GenerateSchemaGraph(string connectionString);
        void GeneratePrimaryKeys(string tableName);
        void GenerateForeignKeys(string tableName);
        void GenerateIndexScript(string tableName, string indexName);
        void GenerateIndexDrop(string tableName, string indexName);
        void GenerateIndexStatistics(string tableName, string indexName);
        List<string> GenerateTableColumns(string tableName);

        void GenerateColumnAddScript(Column column);
        void GenerateColumnDropScript(Column column);
        void GenerateColumnAlterScript(Column column);
        void GenerateColumnSetDefaultScript(Column column);
        void GenerateColumnDropDefaultScript(Column column);

    }
}
