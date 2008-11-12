using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace ExportSQLCE
{
    class Program
    {
        static void Main(string[] args)
        {
            if (args.Length != 2)
                PrintUsageGuide();
            else
            {
                string connectionString = args[0];
                string outputFileLocation = args[1];

                IRepository repository = new DBRepository(connectionString);
                Generator generator = new Generator(repository);

                // The execution below has to be in this sequence
                generator.GenerateTables();
                generator.GenerateTableContent();
                generator.GeneratePrimaryKeys();
                generator.GenerateForeignKeys();
                // Finally added at 26 September 2008, 24 hrs a day are just not enuf :P
                generator.GenerateIndex();
                Console.WriteLine("Generate script Completed");

                Helper.WriteIntoFile(generator.GeneratedScript, outputFileLocation);
                Console.WriteLine("Sent the script into the output file : {0}", outputFileLocation);
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
