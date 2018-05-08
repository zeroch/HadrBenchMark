using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using System.Collections.Generic;
using System.Collections.Concurrent;

namespace TrafficSimulator
{
    class Program
    {
        static void Main(string[] args)
        {
            CancellationTokenSource cancellationTokenSource = new CancellationTokenSource();
            CancellationToken cancellationToken = cancellationTokenSource.Token;
            ConcurrentQueue<Process> cq = new ConcurrentQueue<Process>();
            Console.WriteLine("Hello World!");
            List<string> dbList = new List<string>();
            for (int i = 1; i < 11; i++)
            {
                string dbname = string.Format("DB_{0}", i);
                dbList.Add(dbname);
            }

            foreach(string dbname in dbList)
            {
                var runningTask = Task.Factory.StartNew(() => CreateOstressTask(cq, dbname), cancellationToken);
            }
            Console.ReadKey();
            cancellationTokenSource.Cancel();
            KillOStressTask(cq);
            Console.WriteLine("All task should be canceled.");
            Console.ReadKey();


        }
        static public string DecoratePath(string path)
        {
            char doubleQuaote = '\"';
            string ret = doubleQuaote + path + doubleQuaote;
            return ret;
        }
        public static void CreateOstressTask(ConcurrentQueue<Process> cq, string dbName)
        {
            Process ostress = new Process();
            ostress.StartInfo.FileName = @"C:\Program Files\Microsoft Corporation\RMLUtils\ostress.exe";
            string queryPath = DecoratePath(@"C:\Users\zeche\Documents\Visual Studio 2017\Projects\HadrBenchMark\HadrBenchMark\test_noloop.sql");
            string outputBase = @"C:\temp\";
            string outputPath = DecoratePath(outputBase + dbName);
            string argument = @"-Sze-bench-01\hadrbenchmark01 -d" +dbName + ' ' +  "-r100000 -q -i" + queryPath + " -o" + outputPath;
            Console.WriteLine(DecoratePath(queryPath));
            Console.WriteLine(DecoratePath(outputPath));
            Console.WriteLine(argument);
            ostress.StartInfo.Arguments = argument;
            cq.Enqueue(ostress);
            ostress.Start();
        }
        public static void KillOStressTask(ConcurrentQueue<Process> cq)
        {
            Process ostress;
            while (cq.TryDequeue(out ostress))
            {
                if (!ostress.HasExited)
                {
                    ostress.Kill();
                }
            }
        }
    }
}
