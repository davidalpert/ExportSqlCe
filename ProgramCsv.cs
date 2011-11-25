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
            //AppDomain.CurrentDomain.AssemblyResolve += new ResolveEventHandler(CurrentDomain_AssemblyResolve);
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
                    if (args.Length == 5 && args[4].Length == 1) 
                        separator = Convert.ToChar(args[4]);

                    if (!System.IO.File.Exists(inputFileLocation))
                    {
                        System.Console.WriteLine("Input file not found");
                        return 1;
                    }

                    string outPath = System.IO.Path.GetDirectoryName(outputFileLocation);
                    if (!System.IO.Directory.Exists(outPath))
                    {
                        System.Console.WriteLine("Output folder does not exist");
                        return 1;
                    }

                    using (IRepository repository =  Helper.CreateRepository(connectionString))
                    {
                        Helper.FinalFiles = outputFileLocation;
                        System.Diagnostics.Stopwatch sw = new System.Diagnostics.Stopwatch();
                        sw.Start();
#if V40
                        var generator = new Generator4(repository, outputFileLocation);
#else
                        var generator = new Generator(repository, outputFileLocation);
#endif
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
                catch (System.Data.SqlServerCe.SqlCeException e)
                {
                    Helper.ShowErrors(e);
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

        private static void PrintUsageGuide()
        {
            Console.WriteLine("Usage : ");
            Console.WriteLine(" CsvSQLCE.exe [SQL Server Compact Connection String] [Path to input CSV file] [Path to output SQL Script] [TableName] [Separator]");
            Console.WriteLine(" (Separator is optional, default is comma)");
            Console.WriteLine("");
            Console.WriteLine("Example : ");
            Console.WriteLine(" Csv2SqlCe.exe \"Data Source=C:\\Data\\Northwind.sdf\" \"C:\\Data\\Shippers.csv\" \"C:\\Data\\Shippers.sqlce\" \"Shippers\" \";\" ");
            Console.WriteLine("");
        }
    }
}
