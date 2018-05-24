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
            hadrTestBase.Setup();

            hadrTestBase.ScanDBsFromEnvironment();

            Run(hadrTestBase);

            Console.ReadKey();



        }
        static void Run(HadrTestBase hadrTestBase)
        {
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
                        case "StartTraffic":
                            hadrTestBase.StartTraffic();
                            break;
                        case "RefreshTraffic":
                            hadrTestBase.RefreshTraffic();
                            break;
                        case "DrainTraffic":
                            hadrTestBase.DrainTraffic();
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
