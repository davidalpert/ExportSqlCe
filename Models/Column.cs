using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace ExportSQLCE
{
    struct Column
    {
        public string ColumnName { get; set; }
        public YesNoOptionEnum IsNullable { get; set; }
        public string DataType { get; set; }
        public int CharacterMaxLength { get; set; }
        public int NumericPrecision { get; set; }
        public int NumericScale { get; set; }
        public Int64 AutoIncrementBy { get; set; }
        public Int64 AutoIncrementSeed { get; set; }
        public bool ColumnHasDefault { get; set; }
        public string ColumnDefault { get; set; }
        public bool RowGuidCol { get; set; }
    }
}
