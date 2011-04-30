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
                    bool sqlAzure = false;

                    System.Diagnostics.Stopwatch sw = new System.Diagnostics.Stopwatch();
                    sw.Start();
                            
                    if (args[0].Equals("diff", StringComparison.OrdinalIgnoreCase))
                    {
#if V31
                        PrintUsageGuide();
                        return 2;                        
#else
                        if (args.Length == 4)
                        {
                            using (var source = Helper.CreateRepository(args[1]))
                            {
                                using (var target = Helper.CreateRepository(args[2]))
                                {
                                    //Must have SQL Compact as target or source
                                    if (target.GetType() == typeof(DBRepository) || source.GetType() == typeof(DBRepository))
                                    {
                                        var generator = new Generator(source);
                                        SqlCeDiff.CreateDiffScript(source, target, generator);
                                        System.IO.File.WriteAllText(generator.GeneratedScript, args[3]);
                                        return 0;
                                    }
                                }
                            }
                            return 1;
                        }
                        else
                        {
                            PrintUsageGuide();
                            return 2;
                        }
#endif
                    }
                    else if (args[0].Equals("dgml", StringComparison.OrdinalIgnoreCase))
                    {
#if V31
                        PrintUsageGuide();
                        return 2;
#else
                        if (args.Length == 3)
                        {
                            using (var source = Helper.CreateRepository(args[1]))
                            {
                                var generator = new Generator(source, args[2]);
                                generator.GenerateSchemaGraph(args[1]);
                            }
                            return 0;
                        }
                        else
                        {
                            PrintUsageGuide();
                            return 2;
                        }
#endif                        
                    }
                    else
                    {
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
                            var generator = new Generator(repository, outputFileLocation, sqlAzure);

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
                                generator.GenerateTableContent(saveImageFiles);
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
                        }
                        Console.WriteLine("Sent script to output file(s) : {0} in {1} ms", Helper.FinalFiles, (sw.ElapsedMilliseconds).ToString());
                        return 0;
                    }
                }
                catch (System.Data.SqlServerCe.SqlCeException e)
                {
                    Console.WriteLine(Helper.ShowErrors(e));
                    return 1;
                }
                catch (System.Data.SqlClient.SqlException es)
                {
                    Console.WriteLine(Helper.ShowErrors(es)); 
                    return 1;
                }

                catch (Exception ex)
                {
                    Console.WriteLine("Error: " + ex);
                    return 1;
                }
            }
        }

        private static void PrintUsageGuide()
        {
            Console.WriteLine("Usage : (To script an entire database)");
            Console.WriteLine(" ExportSQLCE.exe [SQL CE Connection String] [output file location] [schemaonly] [saveimages] [sqlazure]");
            Console.WriteLine(" (schemaonly, saveimages and sqlazure are optional parameters)");
            Console.WriteLine("");
            Console.WriteLine("Example : ");
            Console.WriteLine(" ExportSQLCE.exe \"Data Source=D:\\Northwind.sdf;\" Northwind.sql");
            Console.WriteLine("");
            Console.WriteLine("");
#if V31
#else
            Console.WriteLine("Usage: (To create a schema diff script)");
            Console.WriteLine(" ExportSQLCE.exe diff [SQL Compact or SQL Server Connection String (source)] ");
            Console.WriteLine(" [SQL Compact or SQL Server Connection String (target)] [output file location]");
            Console.WriteLine("Example :");
            Console.WriteLine(" ExportSQLCE.exe diff \"Data Source=D:\\Northwind.sdf;\" \"Data Source=.\\SQLEXPRESS,Inital Catalog=Northwind\" NorthwindDiff.sql");
            Console.WriteLine("");
            Console.WriteLine("");

            Console.WriteLine("Usage: (To create a database graph)");
            Console.WriteLine(" ExportSQLCE.exe dgml [SQL Compact or SQL Server Connection String (source)] [output file location]");
            Console.WriteLine("Example :");
            Console.WriteLine(" ExportSQLCE.exe dgml \"Data Source=D:\\Northwind.sdf;\" C:\\temp\\northwind.dgml");
#endif
        }
    }
}
