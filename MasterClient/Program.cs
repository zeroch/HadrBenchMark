using System;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Collections.Generic;
using System.Runtime.Serialization.Formatters.Binary;
using System.IO;
using MasterSlaveController;
using System.Threading;

public class Client
{


    public class MessageClient
    {
        private int _port;
        private Dictionary<string, TcpClient> _clientsDictionary;
        private bool _running, _disposed;
        BinaryFormatter _bFormatter;
        private Thread _connectionThread;

        public MessageClient(int port)
        {
            this._port = port;
            this._clientsDictionary = new Dictionary<string, TcpClient>();
            this._running = false;
            this._bFormatter = new BinaryFormatter();
        }

        public void Setup()
        {
            // here is setup entrypoint to create default tcpclient target
            if (!Connect("zeche-traffic1"))
            {
                Console.WriteLine("Connect to zecheTraffic1 failed");
            }
            if (!Connect("zeche-traffic2"))
            {
                Console.WriteLine("Connect to zecheTraffic2 failed");
            }
        }

        public void Close()
        {
            Message opMessage = GetMessage("Close");

            foreach (var keyValuePair in _clientsDictionary)
            {
                TcpClient client = keyValuePair.Value;
                string server = keyValuePair.Key;
                SendMessageToServer(server, opMessage);
                client.Close();
                client.Dispose();
            }
            _clientsDictionary.Clear();
        }


        // if there is existed connection, close it first then establish a new one.
        public bool Connect(string server)
        {
            TcpClient client = null;
            if (_clientsDictionary.ContainsKey(server))
            {
                client = _clientsDictionary[server];
                client.Close();
            }

            client = new TcpClient(server, _port);
            if (client != null)
            {
                _clientsDictionary[server] = client;
                return true;
            }
            else
            {
                return false;
            }
        }
        public void SendMessageToServer(string server, Message message)
        {
            TcpClient client = null;

            if (_clientsDictionary.ContainsKey(server))
            {
                client = _clientsDictionary[server];

                if (client != null)
                {
                    _bFormatter.Serialize(client.GetStream(), message);
                }

            }
        }

        public Message GetMessage(List<String> dblist)
        {
            using (var memoryStream = new MemoryStream())
            {
                (new BinaryFormatter()).Serialize(memoryStream, dblist);
                return new Message { Type = MessageType.DB, Data = memoryStream.ToArray() };
            }

        }
        public Message GetMessage(string msg)
        {
            if (msg.Equals("Close"))
            {
                using (var memoryStream = new MemoryStream())
                {
                    (new BinaryFormatter()).Serialize(memoryStream, msg);
                    return new Message { Type = MessageType.Info, Data = memoryStream.ToArray() };
                }
            }
            else
            {
                using (var memoryStream = new MemoryStream())
                {
                    (new BinaryFormatter()).Serialize(memoryStream, msg);
                    return new Message { Type = MessageType.operation, Data = memoryStream.ToArray() };
                }
            }
        }

    }





    //static void Connect(String server)
    //{
    //    try
    //    {
    //        // Create a TcpClient.
    //        // Note, for this client to work you need to have a TcpServer 
    //        // connected to the same address as specified by the server, port
    //        // combination.
    //        Int32 port = 11000;
    //        TcpClient client = new TcpClient(server, port);
    //        BinaryFormatter _bFormatter = new BinaryFormatter();

    //        // 1-3
    //        List<string> dblist = new List<string>();
    //        for (int i = 1; i < 4; i++)
    //        {
    //            string dbname = String.Format("DB_{0}", i);
    //            dblist.Add(dbname);
    //        }

    //        // Translate the passed message into ASCII and store it as a Byte array.
    //        //Byte[] data = System.Text.Encoding.ASCII.GetBytes(message);

    //        // send a message to trigger 3 database traffic
    //        Message dbMessage = GetMessage(dblist);
    //        _bFormatter.Serialize(client.GetStream(), dbMessage);

    //        Console.ReadLine();
    //        dblist.Clear();

    //        // send another  4-9    6 dbs
    //        for (int i = 4; i < 10; i++)
    //        {
    //            string dbname = String.Format("DB_{0}", i);
    //            dblist.Add(dbname);
    //        }
    //         dbMessage = GetMessage(dblist);
    //        _bFormatter.Serialize(client.GetStream(), dbMessage);
    //        // Send the message to the connected TcpServer. 
    //        //stream.Write(data, 0, data.Length);




    //        Console.ReadKey();
    //        // close all traffic
    //        Message opMessage = GetMessage("Close");
    //        _bFormatter.Serialize(client.GetStream(), opMessage);

    //        Console.ReadKey();

    //        // Receive the TcpServer.response.

    //        // Buffer to store the response bytes.
    //        //data = new Byte[256];

    //        // String to store the response ASCII representation.
    //        //String responseData = String.Empty;

    //        // Read the first batch of the TcpServer response bytes.
    //        //Int32 bytes = stream.Read(data, 0, data.Length);
    //        //responseData = System.Text.Encoding.ASCII.GetString(data, 0, bytes);

    //        //List<string> receivedDBList = (List<String>)_bFormatter.Deserialize(client.GetStream());
    //        //if (receivedDBList != null)
    //        //{
    //        //    Console.WriteLine("Received: {0}", receivedDBList);
    //        //}

    //        // signal server to close this connection
    //        opMessage = GetMessage("Close");
    //        _bFormatter.Serialize(client.GetStream(), opMessage);
    //        client.Close();

    //}
    //    catch (ArgumentNullException e)
    //    {
    //        Console.WriteLine("ArgumentNullException: {0}", e);
    //    }
    //    catch (SocketException e)
    //    {
    //        Console.WriteLine("SocketException: {0}", e);
    //    }

    //    Console.WriteLine("\n Press Enter to continue...");
    //    Console.Read();
    //}



    public static int Main(String[] args)
    {
        Console.WriteLine("Entry point");
        MessageClient mclient = new MessageClient(11000);
        mclient.Setup();
        Console.WriteLine("complete connection");
        Console.ReadLine();
        List<string> dblist = new List<string>();
        for (int i = 1; i < 3; i++)
        {
            string dbname = String.Format("DB_{0}", i);
            dblist.Add(dbname);
        }
        Message dbMessage = mclient.GetMessage(dblist);
        mclient.SendMessageToServer("zeche-traffic1", dbMessage);

        Console.ReadLine();
        dblist.Clear();

        for (int i = 3; i < 5; i++)
        {
            string dbname = String.Format("DB_{0}", i);
            dblist.Add(dbname);
        }
        dbMessage = mclient.GetMessage(dblist);
        mclient.SendMessageToServer("zeche-traffic2", dbMessage);

        Console.ReadLine();
        dblist.Clear();

        for (int i = 5; i < 8; i++)
        {
            string dbname = String.Format("DB_{0}", i);
            dblist.Add(dbname);
        }
        dbMessage = mclient.GetMessage(dblist);
        mclient.SendMessageToServer("zeche-traffic1", dbMessage);

        Console.ReadLine();
        dblist.Clear();


        // send another  4-9    6 dbs
        for (int i = 8; i < 10; i++)
        {
            string dbname = String.Format("DB_{0}", i);
            dblist.Add(dbname);
        }
        dbMessage = mclient.GetMessage(dblist);
        mclient.SendMessageToServer("zeche-traffic2", dbMessage);

        Console.ReadLine();
        mclient.Close();



        return 0;
    }
}
