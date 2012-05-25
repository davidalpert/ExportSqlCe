using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace ErikEJ.SqlCeScripting
{
    public sealed class SqlCeDiff
    {
        private SqlCeDiff()
        {}

        public static void CreateDiffScript(IRepository sourceRepository, IRepository targetRepository,IGenerator generator, bool includeTargetDrops)
        {
            List<string> sourceTables = sourceRepository.GetAllTableNames();
            List<string> targetTables = targetRepository.GetAllTableNames();

            // Script each table not in the target
            foreach (string tableName in sourceTables.Except(targetTables))
            {
                generator.GenerateTableCreate(tableName);
            }
            foreach (string tableName in sourceTables.Except(targetTables))
            {
                generator.GeneratePrimaryKeys(tableName);
            }
            foreach (string tableName in sourceTables.Except(targetTables))
            {
                List<string> tableIndexes = sourceRepository.GetIndexesFromTable(tableName).Select(i => i.IndexName).Distinct().ToList();
                foreach (var index in tableIndexes)
                {
                    generator.GenerateIndexScript(tableName, index);
                }
            }
            foreach (string tableName in sourceTables.Except(targetTables))
            {
                generator.GenerateForeignKeys(tableName);
            }

            // Drop each table in the target but not the source
            if (includeTargetDrops)
            {
                foreach (string tableName in targetTables.Except(sourceTables))
                {
                    generator.GenerateTableDrop(tableName);
                }
            }

            //For each table both in target and source
            foreach (string tableName in sourceTables.Intersect(targetTables))
            {
                // Check columns for the table: Dropped, added or changed ?
                IEnumerable<Column> sourceColumns = from c in sourceRepository.GetAllColumns()
                                    where c.TableName == tableName
                                    select c;
                IEnumerable<Column> targetColumns = from c in targetRepository.GetAllColumns()
                                    where c.TableName == tableName
                                    select c;

                // Added columns
                foreach (var column in sourceColumns.Except(targetColumns, new ColumnComparer()))
                {
                    generator.GenerateColumnAddScript(column);
                }
                // Same columns, check for changes
                foreach (var sourceColumn in sourceColumns.Intersect(targetColumns, new ColumnComparer()))
                {
                    bool altered = false;
                    // Check if they have any differences:
                    var targetColumn = (from c in targetColumns
                                        where c.TableName == sourceColumn.TableName && c.ColumnName == sourceColumn.ColumnName
                                        select c).Single();
                    if (sourceColumn.IsNullable != targetColumn.IsNullable)
                        altered = true;
                    if (sourceColumn.NumericPrecision != targetColumn.NumericPrecision)
                        altered = true;
                    if (sourceColumn.NumericScale != targetColumn.NumericScale)
                        altered = true;
                    if (sourceColumn.AutoIncrementBy != targetColumn.AutoIncrementBy)
                        altered = true;
                    if (sourceColumn.CharacterMaxLength != targetColumn.CharacterMaxLength)
                        altered = true;
                    if (sourceColumn.DataType != targetColumn.DataType)
                        altered = true;

                    if (altered)
                        generator.GenerateColumnAlterScript(sourceColumn);

                    // Changed defaults is special case
                    if (!targetColumn.ColumnHasDefault && sourceColumn.ColumnHasDefault)
                    {
                        generator.GenerateColumnSetDefaultScript(sourceColumn);
                    }
                    if (!sourceColumn.ColumnHasDefault && targetColumn.ColumnHasDefault)
                    {
                        generator.GenerateColumnDropDefaultScript(sourceColumn);
                    }
                    // If both columns have defaults, but they are different
                    if ((sourceColumn.ColumnHasDefault && targetColumn.ColumnHasDefault) && (sourceColumn.ColumnDefault != targetColumn.ColumnDefault))
                    {
                        generator.GenerateColumnSetDefaultScript(sourceColumn);
                    }
                }

                //Check primary keys
                List<PrimaryKey> sourcePK = sourceRepository.GetAllPrimaryKeys().Where(p => p.TableName == tableName).ToList();
                List<PrimaryKey> targetPK = targetRepository.GetAllPrimaryKeys().Where(p => p.TableName == tableName).ToList();

                // Add the PK
                if (targetPK.Count == 0 && sourcePK.Count > 0)
                {
                    generator.GeneratePrimaryKeys(tableName);
                }

                // Do we have the same columns, if not, drop and create.
                if (sourcePK.Count > 0 && targetPK.Count > 0)
                {
                    if (sourcePK.Count == targetPK.Count)
                    {
                        //Compare columns
                        for (int i = 0; i < sourcePK.Count; i++)
                        {
                            if (sourcePK[i].ColumnName != targetPK[i].ColumnName)
                            {
                                generator.GeneratePrimaryKeyDrop(sourcePK[i], tableName);
                                generator.GeneratePrimaryKeys(tableName);
                                break;
                            }
                        }
                    }
                    // Not same column count, just drop and create
                    else
                    {
                        generator.GeneratePrimaryKeyDrop(sourcePK[0], tableName);
                        generator.GeneratePrimaryKeys(tableName);
                    }
                }

                // Check indexes
                List<Index> sourceIXs = sourceRepository.GetIndexesFromTable(tableName);
                List<Index> targetIXs = targetRepository.GetIndexesFromTable(tableName);

                // Check added indexes (by name only)
                foreach (var index in sourceIXs)
                {
                    var targetIX = targetIXs.Where(s => s.IndexName == index.IndexName);
                    if (targetIX.Count() == 0)
                    {
                        generator.GenerateIndexScript(index.TableName, index.IndexName);
                    }
                }

                // Check foreign keys
                List<Constraint> sourceFKs = sourceRepository.GetAllForeignKeys(tableName);
                List<Constraint> targetFKs = targetRepository.GetAllForeignKeys(tableName);

                // Check added foreign keys (by name only)
                foreach (var fk in sourceFKs)
                {
                    Constraint targetFK = targetFKs.Where(s => s.ConstraintName == fk.ConstraintName).SingleOrDefault();
                    if (targetFK == null)
                    {
                        generator.GenerateForeignKey(fk);
                    }
                }
                // Check deleted FKs (by name only)
                foreach (var fk in targetFKs)
                {
                    Constraint sourceFK = sourceFKs.Where(s => s.ConstraintName == fk.ConstraintName).SingleOrDefault();
                    if (sourceFK == null)
                    {
                        generator.GenerateForeignKeyDrop(fk);
                    }
                }

                // Check deleted indexes (by name only)
                foreach (var index in targetIXs)
                {
                    var sourceIX = sourceIXs.Where(s => s.IndexName == index.IndexName);
                    if (sourceIX.Count() == 0)
                    {
                        generator.GenerateIndexOnlyDrop(index.TableName, index.IndexName);
                    }
                }

                // Dropped columns
                foreach (var column in targetColumns.Except(sourceColumns, new ColumnComparer()))
                {
                    generator.GenerateColumnDropScript(column);
                }

            }
        }

        public static string CreateDataDiffScript(IRepository sourceRepository, string sourceTable, IRepository targetRepository, string targetTable, IGenerator generator)
        {
            StringBuilder sb = new StringBuilder();

            List<Column> sourceColumns = (from c in sourceRepository.GetAllColumns()
                                                where c.TableName == sourceTable
                                                select c).ToList();
            List<Column> targetColumns = (from c in targetRepository.GetAllColumns()
                                                where c.TableName == targetTable
                                                select c).ToList();

            var pkList = sourceRepository.GetAllPrimaryKeys().Where(pk => pk.TableName == sourceTable).Select(pk => pk.ColumnName).ToList();
            if (pkList.Count < 1)
            {
                throw new ArgumentException("Source does not have a primary key, this is required");
            }

            if (sourceColumns.Count() != targetColumns.Count())
            {
                throw new ArgumentException("Source and target does not have same number of columns");
            }

            for (int i = 0; i < sourceColumns.Count() - 1; i++)
            {
                if (sourceColumns[i].ShortType != targetColumns[i].ShortType)
                {
                    throw new ArgumentException(string.Format("The columm {0} does not have the expected type {1} in the target table", sourceColumns[i].ColumnName, sourceColumns[i].ShortType));
                }
            }

            using (var rdr = sourceRepository.GetDataFromReader(sourceTable, sourceColumns))
            { 
                while (rdr.Read())
                {
                    //Call a method in the target repository that returns a string with either INSERT or UPDATE statement
                    //Takes 2 parameters, one Dict with primary key columns and values, one Dict with all other columns and values
                    Dictionary<string, object> keys = new Dictionary<string, object>();
                    Dictionary<string, object> values = new Dictionary<string, object>();

                    foreach (var pk in pkList)
                    {
                        keys.Add(pk, rdr[pk]);
                    }

                    for (int i = 0; i < rdr.FieldCount; i++)
                    {
                        //Add all values other than PK values
                        if (!pkList.Contains(rdr.GetName(i)))
                        {
                            values.Add(rdr.GetName(i), rdr[i]);
                        }
                    }
                    //TODO Make repository call here

                    sb.AppendLine("");
                    sb.AppendLine("GO");
                }
            }
            return sb.ToString();
        }

    }
}
