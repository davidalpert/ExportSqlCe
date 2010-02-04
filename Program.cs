using System;
using System.Text;

namespace ExportSqlCE
{
    class Program
    {
        static void Main(string[] args)
        {
            if (args.Length != 2)
                PrintUsageGuide();
            else
            {
                try
                {
                    string connectionString = args[0];
                    string outputFileLocation = args[1];

                    using (IRepository repository = new DBRepository(connectionString))
                    {
                        Helper.FinalFiles = outputFileLocation;
                        System.Diagnostics.Stopwatch sw = new System.Diagnostics.Stopwatch();
                        sw.Start();
                        var generator = new Generator(repository, outputFileLocation);
                        // The execution below has to be in this sequence
                        Console.WriteLine("Generating the tables....");
#if V35
                        generator.GenerateTable(true);
#else
                        generator.GenerateTable(false);
#endif
                        Console.WriteLine("Generating the data....");
                        generator.GenerateTableContent();
                        Console.WriteLine("Generating the primary keys....");
                        generator.GeneratePrimaryKeys();
                        Console.WriteLine("Generating the foreign keys....");
                        generator.GenerateForeignKeys();
                        Console.WriteLine("Generating the indexes....");
                        // Finally added at 26 September 2008, 24 hrs a day are just not enuf :P
                        generator.GenerateIndex();
                        
                        Helper.WriteIntoFile(generator.GeneratedScript, outputFileLocation, generator.FileCounter);
                        Console.WriteLine("Sent script to output file(s) : {0} in {1} ms", Helper.FinalFiles, (sw.ElapsedMilliseconds).ToString());
                    }
                }
                catch (System.Data.SqlServerCe.SqlCeException e)
                {
                    ShowErrors(e);
                }
                catch (Exception ex)
                {
                    Console.WriteLine("Error: " + ex);
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
            Console.WriteLine(" ExportSQLCE.exe [SQL CE Connection String] [output file location]");
            Console.WriteLine("Example : ");
            Console.WriteLine(" ExportSQLCE.exe \"Data Source=D:\\Northwind.sdf;\" Northwind.sql");
        }
    }
}
