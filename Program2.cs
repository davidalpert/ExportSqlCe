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

                    using (IRepository repository = new ServerDBRepository(connectionString))
                    {
                        System.Diagnostics.Stopwatch sw = new System.Diagnostics.Stopwatch();
                        sw.Start();
                        var generator = new Generator(repository, outputFileLocation);
                        // The execution below has to be in this sequence
                        Console.WriteLine("Generating the tables....");
#if V35
                        generator.GenerateTable(true);
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
            Console.WriteLine(" Export2SQLCE.exe [SQL Server Connection String] [output file location]");
            Console.WriteLine("Example : ");
            Console.WriteLine(" Export2SQLCE.exe \"Data Source=(local);Initial Catalog=Northwind;Integrated Security=True\" Northwind.sql");
        }
    }
}
