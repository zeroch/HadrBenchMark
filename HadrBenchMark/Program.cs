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
            Console.WriteLine("Hello World!");
            HadrTestBase hadrTestBase = new HadrTestBase();
            hadrTestBase.Setup();
            hadrTestBase.TestNodesHADREnabled();
            hadrTestBase.TestCreateAGWithTwoReplicasWithoutDatabase();
            Console.ReadKey();
            hadrTestBase.CleanUp();
        }
    }
}
