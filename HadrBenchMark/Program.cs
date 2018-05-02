using System;
using Microsoft.SqlServer.Management.Common;
using Microsoft.SqlServer.Management.Smo;
using System.Collections.Generic;
using System.Data;
using HadrBenchMark;


namespace HadrBenchMark
{
    class Program
    {
        static void Main(string[] args)
        {
            HadrTestBase hadrTestBase = new HadrTestBase();

            //try
            //{
            //    Console.WriteLine("Hello World!");

                hadrTestBase.Setup();
                bool Alive = true;
                do
                {
                    string command = Console.ReadLine();
                    // parse command
                    string[] param = command.Split(' ');
                    if (param != null)
                    {
                        switch (param[0])
                        {
                            case "AddDatabase":
                                string dbNumber = param[1];
                                int numberOfDB = 0;
                                if (Int32.TryParse(dbNumber, out numberOfDB))
                                {
                                    hadrTestBase.AddDatabases(numberOfDB);
                                }
                                else
                                {
                                    Console.WriteLine("invalid number.");
                                }
                                break;
                            case "AddReplica":
                                string arNumber = param[1];
                                int numberOfAR = 0;
                                if (Int32.TryParse(arNumber, out numberOfAR))
                                {
                                    CreateReplica(numberOfAR);
                                }
                                else
                                {
                                    Console.WriteLine("invalid number.");
                                }
                                break;
                            case "Cleanup":
                                Alive = false;
                                hadrTestBase.CleanUp();
                                break;
                            default:
                                Console.WriteLine("No Valid parameter");
                                break;
                        }
                    }

                } while (Alive);
            //}catch
            //{
            //    hadrTestBase.CleanUp();
            //}

            Console.ReadKey();
            //hadrTestBase.CleanUp();
        }

        static void CreateReplica(int num)
        {
            Console.WriteLine("Add {0} Replica into AG", num);
        }
        static void CleanupEnv()
        {
            Console.WriteLine("Clean up environment.");

        }
    }
}
