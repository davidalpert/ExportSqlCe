using System;
using System.Collections.Generic;
using System.Text;
using System.IO;

namespace ExportSQLCE
{
    class Helper
    {
        internal static void WriteIntoFile(string script, string fileLocation, int increment)
        {
            string ext = System.IO.Path.GetExtension(System.IO.Path.GetFileNameWithoutExtension(fileLocation));
            string path = string.Empty;
            if (increment > -1)
            {
                path = fileLocation + "_" + increment.ToString(System.Globalization.CultureInfo.InvariantCulture);
            }
            if (!string.IsNullOrEmpty(ext))
            {
                path = fileLocation + ext;
            }
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
