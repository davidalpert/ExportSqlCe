using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace ExportSqlCE
{
    class ColumnList : List<string>
    {
        public override string ToString()
        {
            StringBuilder formatter = new StringBuilder();
            foreach (string value in this)
            { 
                formatter.Append("[");
                formatter.Append(value);
                formatter.Append("], ");
            }
            if (formatter.Length > 3)
            {
                formatter.Remove(formatter.Length - 2, 2);
            }
            return formatter.ToString();
        }
    }
}
