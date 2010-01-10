using System.Text;
using System.IO;

namespace ExportSqlCE
{
    internal static class Helper
    {
        internal static void WriteIntoFile(string script, string fileLocation, int increment)
        {
            if (increment > -1)
            {
                string ext = Path.GetExtension(System.IO.Path.GetFileName(fileLocation));
                string path = Path.GetDirectoryName(fileLocation);
                string name = Path.GetFileNameWithoutExtension(fileLocation);
                fileLocation = Path.Combine(path, name + "_" + increment.ToString(System.Globalization.CultureInfo.InvariantCulture));
                if (!string.IsNullOrEmpty(ext))
                {
                    fileLocation = fileLocation + ext;
                }
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

        internal static string FixConnectionString(string connectionString, int timeout)
        {
            return connectionString.Replace(string.Format(";Timeout = \"{0}\"", timeout), string.Empty);
        }

        internal static string CheckDataType(string dataType)
        {
            return dataType;
        }

    }
}
