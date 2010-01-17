using System;
using System.Text;

namespace ExportSqlCE
{
    class Program
    {
        static void Main(string[] args)
        {
            if (args.Length < 2 || args.Length > 3)
                PrintUsageGuide();
            else
            {
                try
                {
                    string connectionString = args[0];
                    string outputFileLocation = args[1];

                    bool includeData = true;
                    if (args.Length > 2)
                    {
                        if (args[2].Contains("schemaonly"))
                        {
                            includeData = false;
                        }
                    }

                    using (IRepository repository = new ServerDBRepository(connectionString))
                    {
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
                            generator.GenerateTableContent();
                        }
                        Console.WriteLine("Generating the primary keys....");
                        generator.GeneratePrimaryKeys();
                        Console.WriteLine("Generating the foreign keys....");
                        generator.GenerateForeignKeys();
                        Console.WriteLine("Generating the indexes....");
                        // Finally added at 26 September 2008, 24 hrs a day are just not enuf :P
                        generator.GenerateIndex();
                        
                        Helper.WriteIntoFile(generator.GeneratedScript, outputFileLocation, generator.FileCounter);
                        Console.WriteLine("Sent the script into the output file : {0} in {1} second(s)", outputFileLocation, (sw.ElapsedMilliseconds / 1000).ToString());
                    }
                }
                catch (System.Data.SqlClient.SqlException e)
                {
                    ShowErrors(e);
                }
                catch (Exception ex)
                {
                    Console.WriteLine("Error: " + ex);
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
            Console.WriteLine(" Export2SQLCE.exe [SQL Server Connection String] [output file location] [schemaonly]");
            Console.WriteLine("");
            Console.WriteLine("Examples : ");
            Console.WriteLine(" Export2SQLCE.exe \"Data Source=(local);Initial Catalog=Northwind;Integrated Security=True\" Northwind.sql");
            Console.WriteLine(" Export2SQLCE.exe \"Data Source=(local);Initial Catalog=Northwind;Integrated Security=True\" Northwind.sql schemaonly");
            Console.WriteLine("");
            Console.WriteLine("Server data types currently NOT supported: ");
            Console.WriteLine("date, datetime2, datetimeoffset, sql_variant, time");
        }
    }
}
