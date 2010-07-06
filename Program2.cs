using System;
using System.Text;
using ErikEJ.SqlCeScripting;

namespace ExportSqlCE
{
    class Program
    {
        static int Main(string[] args)
        {
            if (args.Length < 2 || args.Length > 4)
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

                    for (int i = 2; i < args.Length; i++)
                    {
                        if (args[i].Contains("schemaonly"))
                            includeData = false;
                        if (args[i].Contains("saveimages"))
                            saveImageFiles = true;
                    }

                    using (IRepository repository = new ServerDBRepository(connectionString))
                    {
                        Helper.FinalFiles = outputFileLocation;
                        System.Diagnostics.Stopwatch sw = new System.Diagnostics.Stopwatch();
                        sw.Start();
                        var generator = new Generator(repository, outputFileLocation);
                        // The execution below has to be in this sequence
                        Console.WriteLine("Generating the tables....");
#if V35
                        generator.GenerateTable(includeData);
#endif
                        if (includeData)
                        {
                            Console.WriteLine("Generating the data....");
                            generator.GenerateTableContent(saveImageFiles);
                        }
                        Console.WriteLine("Generating the primary keys....");
                        generator.GeneratePrimaryKeys();
                        Console.WriteLine("Generating the indexes....");
                        generator.GenerateIndex();
                        Console.WriteLine("Generating the foreign keys....");
                        generator.GenerateForeignKeys();

                        Helper.WriteIntoFile(generator.GeneratedScript, outputFileLocation, generator.FileCounter);
                        Console.WriteLine("Sent script to output file(s) : {0} in {1} ms", Helper.FinalFiles, (sw.ElapsedMilliseconds).ToString());
                        return 0;
                    }
                }
                catch (System.Data.SqlClient.SqlException e)
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

        private static void ShowErrors(System.Data.SqlClient.SqlException e)
        {
            System.Data.SqlClient.SqlErrorCollection errorCollection = e.Errors;

            StringBuilder bld = new StringBuilder();
            Exception inner = e.InnerException;

            if (null != inner)
            {
                Console.WriteLine("Inner Exception: " + inner.ToString());
            }
            // Enumerate the errors to a message box.
            foreach (System.Data.SqlClient.SqlError err in errorCollection)
            {
                bld.Append("\n Message   : " + err.Message);
                bld.Append("\n Source    : " + err.Source);
                bld.Append("\n Number    : " + err.Number);

                Console.WriteLine(bld.ToString());
                bld.Remove(0, bld.Length);
            }
        }

        private static void PrintUsageGuide()
        {
            Console.WriteLine("Usage : ");
            Console.WriteLine(" Export2SQLCE.exe [SQL Server Connection String] [output file location] [[schemaonly]] [[saveimages]]");
            Console.WriteLine(" (schemaonly and saveimages are optional parameters)");
            Console.WriteLine("");
            Console.WriteLine("Examples : ");
            Console.WriteLine(" Export2SQLCE.exe \"Data Source=(local);Initial Catalog=Northwind;Integrated Security=True\" Northwind.sql");
            Console.WriteLine(" Export2SQLCE.exe \"Data Source=(local);Initial Catalog=Northwind;Integrated Security=True\" Northwind.sql schemaonly");
            Console.WriteLine("");
            Console.WriteLine("Server data types currently NOT supported: ");
            Console.WriteLine(" sql_variant");
        }
    }
}
