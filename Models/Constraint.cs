using System;
using System.Collections.Generic;
using System.Text;

namespace ExportSQLCE
{
    struct Constraint
    {
        public string ConstraintTableName { get; set; }
        public string ConstraintName { get; set; }
        public string ColumnName { get; set; }
        public string UniqueConstraintTableName { get; set; }
        public string UniqueConstraintName { get; set; }
        public string UniqueColumnName { get; set; }
    }
}
