using System;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Collections.Generic;
using System.Runtime.Serialization.Formatters.Binary;
using System.IO;
using MasterSlaveController;

public class Client
{
    static void Connect(String server)
    {
        try
        {
            // Create a TcpClient.
            // Note, for this client to work you need to have a TcpServer 
            // connected to the same address as specified by the server, port
            // combination.
            Int32 port = 11000;
            TcpClient client = new TcpClient(server, port);

            List<string> dblist = new List<string>();
            for (int i = 1; i < 10; i++)
            {
                string dbname = String.Format("DB_{0}", i);
                dblist.Add(dbname);
            }

            // Translate the passed message into ASCII and store it as a Byte array.
            //Byte[] data = System.Text.Encoding.ASCII.GetBytes(message);

            // Get a client stream for reading and writing.
            //  Stream stream = client.GetStream();
            Message dbMessage = GetMessage(dblist);
            NetworkStream stream = client.GetStream();

            // Send the message to the connected TcpServer. 
            //stream.Write(data, 0, data.Length);
            BinaryFormatter _bFormatter = new BinaryFormatter();
            _bFormatter.Serialize(client.GetStream(), dbMessage);

            //Console.WriteLine("Sent: {0}", message);

            // Receive the TcpServer.response.

            // Buffer to store the response bytes.
            //data = new Byte[256];

            // String to store the response ASCII representation.
            String responseData = String.Empty;

            // Read the first batch of the TcpServer response bytes.
            //Int32 bytes = stream.Read(data, 0, data.Length);
            //responseData = System.Text.Encoding.ASCII.GetString(data, 0, bytes);

            //List<string> receivedDBList = (List<String>)_bFormatter.Deserialize(client.GetStream());
            //if (receivedDBList != null)
            //{
            //    Console.WriteLine("Received: {0}", receivedDBList);
            //}


            Console.WriteLine("Received: {0}", responseData);

            // Close everything.
            stream.Close();
            client.Close();
        }
        catch (ArgumentNullException e)
        {
            Console.WriteLine("ArgumentNullException: {0}", e);
        }
        catch (SocketException e)
        {
            Console.WriteLine("SocketException: {0}", e);
        }

        Console.WriteLine("\n Press Enter to continue...");
        Console.Read();
    }


    public static Message GetMessage(List<String> dblist)
    {
        using (var memoryStream = new MemoryStream())
        {
            (new BinaryFormatter()).Serialize(memoryStream, dblist);
            return new Message { Type = MessageType.DB, Data = memoryStream.ToArray() };
        }

    }
    public static Message GetMessage(string msg)
    {
        using (var memoryStream = new MemoryStream())
        {
            (new BinaryFormatter()).Serialize(memoryStream, msg);
            return new Message { Type = MessageType.operation, Data = memoryStream.ToArray() };
        }
    }
    public static int Main(String[] args)
    {
        Connect("zeche013117");
        return 0;
    }
}
