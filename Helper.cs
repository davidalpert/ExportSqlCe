using System;
using System.Collections.Generic;
using System.Text;
using System.IO;

namespace ExportSQLCE
{
    class Helper
    {
        internal static void WriteIntoFile(string script, string fileLocation)
        {
            using (FileStream fs = new FileStream(fileLocation, FileMode.Create, FileAccess.Write, FileShare.Read))
            {
                using (StreamWriter sw = new StreamWriter(fs, Encoding.Unicode))
                {
                    sw.WriteLine(script);
                    sw.Flush();
                }
            }
        }
    }
}
