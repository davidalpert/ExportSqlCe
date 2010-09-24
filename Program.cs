using System;
using System.Text;
using ErikEJ.SqlCeScripting;

namespace ExportSqlCE
{
    class Program
    {
        static int Main(string[] args)
        {
            if (args.Length < 2 || args.Length > 5)
            {
                PrintUsageGuide();
                return 2;
            }
            else
            {
                try
                {
                    string connectionString = args[0];
                    string outputFileLocation = args[1];

                    bool includeData = true;
                    bool saveImageFiles = false;
                    bool generateGraph = false;
                    bool sqlAzure = false;

                    for (int i = 2; i < args.Length; i++)
                    {
                        if (args[i].Contains("schemaonly"))
                            includeData = false;
                        if (args[i].Contains("saveimages"))
                            saveImageFiles = true;
                        if (args[i].Contains("sqlazure"))
                            sqlAzure = true;
                    }

                    using (IRepository repository = new DBRepository(connectionString))
                    {

                        Helper.FinalFiles = outputFileLocation;
                        System.Diagnostics.Stopwatch sw = new System.Diagnostics.Stopwatch();
                        sw.Start();
                        var generator = new Generator(repository, outputFileLocation);

                        Console.WriteLine("Generating the tables....");
#if V31
                        generator.GenerateTable(false);
#else
                        generator.GenerateTable(includeData);
#endif
                        if (sqlAzure)
                        {
                            Console.WriteLine("Generating the primary keys (SQL Azure)....");
                            generator.GeneratePrimaryKeys();
                        }
                        if (includeData)
                        {
                            Console.WriteLine("Generating the data....");
                            generator.GenerateTableContent(saveImageFiles, sqlAzure);
                        }
                        if (!sqlAzure)
                        {
                            Console.WriteLine("Generating the primary keys....");
                            generator.GeneratePrimaryKeys();
                        }
                        Console.WriteLine("Generating the indexes....");
                        generator.GenerateIndex();
                        Console.WriteLine("Generating the foreign keys....");
                        generator.GenerateForeignKeys();

                        Helper.WriteIntoFile(generator.GeneratedScript, outputFileLocation, generator.FileCounter);
                        Console.WriteLine("Sent script to output file(s) : {0} in {1} ms", Helper.FinalFiles, (sw.ElapsedMilliseconds).ToString());
                        return 0;
                    }
                }
                catch (System.Data.SqlServerCe.SqlCeException e)
                {
                    ShowErrors(e);
                    return 1;
                }
                catch (Exception ex)
                {
                    Console.WriteLine("Error: " + ex);
                    return 1;
                }
            }
        }

        private static void ShowErrors(System.Data.SqlServerCe.SqlCeException e)
        {
            System.Data.SqlServerCe.SqlCeErrorCollection errorCollection = e.Errors;

            StringBuilder bld = new StringBuilder();
            Exception inner = e.InnerException;

            if (null != inner)
            {
                Console.WriteLine("Inner Exception: " + inner.ToString());
            }
            // Enumerate the errors to a message box.
            foreach (System.Data.SqlServerCe.SqlCeError err in errorCollection)
            {
                bld.Append("\n Error Code: " + err.HResult.ToString("X", System.Globalization.CultureInfo.InvariantCulture));
                bld.Append("\n Message   : " + err.Message);
                bld.Append("\n Minor Err.: " + err.NativeError);
                bld.Append("\n Source    : " + err.Source);

                // Enumerate each numeric parameter for the error.
                foreach (int numPar in err.NumericErrorParameters)
                {
                    if (0 != numPar) bld.Append("\n Num. Par. : " + numPar);
                }

                // Enumerate each string parameter for the error.
                foreach (string errPar in err.ErrorParameters)
                {
                    if (!string.IsNullOrEmpty(errPar)) bld.Append("\n Err. Par. : " + errPar);
                }

                Console.WriteLine(bld.ToString());
                bld.Remove(0, bld.Length);
            }
        }

        private static void PrintUsageGuide()
        {
            Console.WriteLine("Usage : ");
            Console.WriteLine(" ExportSQLCE.exe [SQL CE Connection String] [output file location] [schemaonly] [saveimages] [sqlazure]");
            Console.WriteLine(" (schemaonly, saveimages and sqlazure are optional parameters)");
            Console.WriteLine("");
            Console.WriteLine("Example : ");
            Console.WriteLine(" ExportSQLCE.exe \"Data Source=D:\\Northwind.sdf;\" Northwind.sql");
        }
    }
}
