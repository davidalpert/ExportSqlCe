﻿using System;

namespace ExportSqlCE
{
    class Column
    {
        public string ColumnName { get; set; }
        public YesNoOption IsNullable { get; set; }
        public string DataType { get; set; }
        public int CharacterMaxLength { get; set; }
        public int NumericPrecision { get; set; }
        public int NumericScale { get; set; }
        public Int64 AutoIncrementBy { get; set; }
        public Int64 AutoIncrementSeed { get; set; }
        public Int64 AutoIncrementNext { get; set; }
        public bool ColumnHasDefault { get; set; }
        public string ColumnDefault { get; set; }
        public bool RowGuidCol { get; set; }
        public string TableName { get; set; }
        public string ShortType
        {
            get

            {
                if (this.DataType == "nchar" || this.DataType == "nvarchar" || this.DataType == "binary" || this.DataType == "varbinary")
                {
                    return string.Format("{0}({1}),", this.DataType, this.CharacterMaxLength);
                }
                else if (this.DataType == "numeric")
                {
                    return string.Format("{0}({1},{2}),", this.DataType, this.NumericPrecision, this.NumericScale);
                }
                else
                {
                    return string.Format("{0},", this.DataType);
                }
            }
        }
    }
}
