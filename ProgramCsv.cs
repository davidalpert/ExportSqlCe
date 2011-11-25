using System;
using System.Text;
using ErikEJ.SqlCeScripting;
using Kent.Boogaart.KBCsv;
using System.Reflection;

namespace Csv2SqlCe
{
    class Program
    {
        static int Main(string[] args)
        {
            AppDomain.CurrentDomain.AssemblyResolve += new ResolveEventHandler(CurrentDomain_AssemblyResolve);
            if (args.Length < 4 || args.Length > 5)
            {
                PrintUsageGuide();
                return 2;
            }
            else
            {
                try
                {
                    string connectionString = args[0];
                    string inputFileLocation = args[1];
                    string outputFileLocation = args[2];
                    string tableName = args[3];

                    Char separator = ',';
                    //TODO add check of args4
                    if (args.Length == 5 && args[4].Length == 1) 
                        separator = Convert.ToChar(args[4]);

                    if (!System.IO.File.Exists(inputFileLocation))
                    {
                        System.Console.WriteLine("Input file not found");
                        return 1;
                    }

                    using (IRepository repository = new DBRepository(connectionString))
                    {
                        Helper.FinalFiles = outputFileLocation;
                        System.Diagnostics.Stopwatch sw = new System.Diagnostics.Stopwatch();
                        sw.Start();
                        var generator = new Generator(repository, outputFileLocation);

                        generator.AddIdentityInsert(tableName);

                        using (var reader = new CsvReader(inputFileLocation))
                        {
                            reader.ValueSeparator = separator;
                            HeaderRecord hr = reader.ReadHeaderRecord();
                            if (generator.ValidColumns(tableName, hr.Values))
                            {
                                foreach (DataRecord record in reader.DataRecords)
                                {
                                    generator.GenerateTableInsert(tableName, hr.Values, record.Values);
                                }
                            }
                        }
                        
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

        static System.Reflection.Assembly CurrentDomain_AssemblyResolve(object sender, ResolveEventArgs args)
        {
            string[] names = Assembly.GetExecutingAssembly().GetManifestResourceNames();

            String resourceName = "Csv2SqlCe." +
               new AssemblyName(args.Name).Name + ".dll";

            using (var stream = Assembly.GetExecutingAssembly().GetManifestResourceStream(resourceName))
            {
                if (stream != null)
                {
                    Byte[] assemblyData = new Byte[stream.Length];
                    stream.Read(assemblyData, 0, assemblyData.Length);
                    return Assembly.Load(assemblyData);
                }
                return null;
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
            Console.WriteLine(" CsvSQLCE.exe [SQL Server Compact Connection String] [Path to CSV file] [Path to SQL File] [TableName] [Separator]");
            Console.WriteLine(" (Separator is optional, default is comma)");
            Console.WriteLine("");
            Console.WriteLine("Examples : ");
            //Console.WriteLine(" Export2SQLCE.exe \"Data Source=(local);Initial Catalog=Northwind;Integrated Security=True\" Northwind.sql");
            //Console.WriteLine(" Export2SQLCE.exe \"Data Source=(local);Initial Catalog=Northwind;Integrated Security=True\" Northwind.sql schemaonly");
            Console.WriteLine("");
        }
    }
}
