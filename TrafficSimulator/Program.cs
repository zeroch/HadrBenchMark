using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Text;

using System.Runtime.Serialization.Formatters.Binary;
using MasterSlaveController;

namespace TrafficSimulator
{
    class Program
    {
        static void Main(string[] args)
        {
            ConcurrentQueue<Process> cq = new ConcurrentQueue<Process>();

            TcpListener server = null;
            try
            {
                // Set the TcpListener on port 13000.
                Int32 port = 11000;
                IPAddress localAddr = IPAddress.IPv6Any;

                // TcpListener server = new TcpListener(port);
                server = new TcpListener(localAddr, port);

                // Start listening for client requests.
                server.Start();

                // Buffer for reading data
                Byte[] bytes = new Byte[256];


                // Enter the listening loop.
                while (true)
                {
                    Console.Write("Waiting for a connection... ");

                    // Perform a blocking call to accept requests.
                    // You could also user server.AcceptSocket() here.
                    TcpClient client = server.AcceptTcpClient();
                    IPEndPoint localEndPoint = (IPEndPoint)client.Client.RemoteEndPoint;

                    IPHostEntry hostEntry = Dns.GetHostEntry(localEndPoint.Address);

                    Console.WriteLine("Connected from {0}!", hostEntry.HostName);

                    CancellationTokenSource cancellationTokenSource = new CancellationTokenSource();

                    // Get a stream object for reading and writing
                    //NetworkStream stream = client.GetStream();
                    while (true)
                    {
                        BinaryFormatter binaryFormatter = new BinaryFormatter();
                        Message msg = (Message)binaryFormatter.Deserialize(client.GetStream());

                        if (msg.Type == MessageType.DB)
                        {
                            List<string> dblist = (List<string>)DecerializeMessage(msg);
                            foreach (string db in dblist)
                            {
                                Console.WriteLine(db);

                            }
                            NewConnectionsToDBs(cq, dblist, cancellationTokenSource);
                        }
                        else if (msg.Type == MessageType.operation)
                        {
                            string command = (string)DecerializeMessage(msg);
                            Console.WriteLine("Receive command: {0}", command);
                        }
                        else if (msg.Type == MessageType.Info)
                        {
                            break;
                        }

                    }



                    //int i;

                    //// Loop to receive all the data sent by the client.
                    //while ((i = stream.Read(bytes, 0, bytes.Length)) != 0)
                    //{
                    //    // Translate data bytes to a ASCII string.
                    //    data = System.Text.Encoding.ASCII.GetString(bytes, 0, i);
                    //    Console.WriteLine("Received: {0}", data);

                    //    // Process the data sent by the client.
                    //    data = data.ToUpper();

                    //    byte[] msg = System.Text.Encoding.ASCII.GetBytes(data);

                    //    // Send back a response.
                    //    stream.Write(msg, 0, msg.Length);
                    //    Console.WriteLine("Sent: {0}", data);
                    //}
                    cancellationTokenSource.Cancel();
                    KillOStressTask(cq);
                    Console.WriteLine("All task should be canceled.");
                    // Shutdown and end connection
                    client.Close();
                    cancellationTokenSource.Dispose();
                }
            }
            catch (SocketException e)
            {
                Console.WriteLine("SocketException: {0}", e);
            }
            finally
            {
                // Stop listening for new clients.
                server.Stop();
            }


            Console.WriteLine("\nHit enter to continue...");
            Console.Read();


            Console.ReadKey();

        }

        static void NewConnectionsToDBs(ConcurrentQueue<Process> cq, List<string> list, CancellationTokenSource cancellationTokenSource)
        {
            CancellationToken cancellationToken = cancellationTokenSource.Token;
            foreach (string dbname in list)
            {
                var runningTask = Task.Factory.StartNew(() => CreateOstressTask(cq, dbname), cancellationToken);
            }
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
            string queryPath = DecoratePath(@"test_noloop.sql");
            string outputBase = @"C:\temp\";
            string outputPath = DecoratePath(outputBase + dbName);
            string argument = @"-Sze-bench-01\hadrbenchmark01 -d" +dbName + ' ' +  "-r10000000 -q -i" + queryPath + " -T146 -o" + outputPath;
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

        public static object DecerializeMessage(Message msg)
        {
            using (var memoryStream = new MemoryStream(msg.Data))
            {
                return (new BinaryFormatter()).Deserialize(memoryStream);
            }
        }
    }
}
