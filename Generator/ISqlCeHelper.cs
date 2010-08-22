using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace ErikEJ.SqlCeScripting
{
    public interface ISqlCeHelper
    {
        string FormatError(Exception ex);
        string GetFullConnectionString(string connectionString);
    }
}
