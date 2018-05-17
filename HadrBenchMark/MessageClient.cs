using System;
using System.Collections.Generic;
using System.Text;
using System.Runtime.Serialization.Formatters.Binary;
using System.IO;
using MasterSlaveController;
using System.Threading;
using System.Net.Sockets;

namespace HadrBenchMark
{
    public class MessageClient
    {
        private int _port;
        private Dictionary<string, TcpClient> _clientsDictionary;

        BinaryFormatter _bFormatter;

        private List<string> serverList;
        private int nextServerIndex;

        public MessageClient(int port)
        {
            this._port = port;
            this._clientsDictionary = new Dictionary<string, TcpClient>();
            this._bFormatter = new BinaryFormatter();
            this.serverList = new List<string>();
            this.nextServerIndex = 0;
        }

        public void Setup()
        {
            // here is setup entrypoint to create default tcpclient target
            serverList.Add("zeche-traffic1");
            serverList.Add("zeche-traffic2");
            serverList.Add("zeche-traffic3");
            serverList.Add("zeche-traffic4");

            foreach (string server in serverList)
            {
                if (!Connect(server))
                {
                    Console.WriteLine("Connect to {0} failed", server);
                }
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

            int retry = 0;
            while(client == null && retry < 3)
            {
                client = new TcpClient(server, _port);
                retry += 1;
            }

            
            if (client != null)
            {
                _clientsDictionary[server] = client;
                return true;
            }
            else
            {
                if (retry >=3 )
                {
                    Console.WriteLine("Failed to connect to {0}", server);
                }
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

        public void SendDbMessage(List<string> list)
        {
            string targetServer = GetServerName();

            if (targetServer != string.Empty)
            {
                // send dblist to server
                Message msg = GetMessage(list);
                SendMessageToServer(targetServer, msg);
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

        // determine which slave to simulate traffic
        // Use round robin to choose slave server
        public string GetServerName()
        {
            int totalServers = serverList.Count;
            int serverIndex = nextServerIndex % totalServers;
            if (serverList.Count > 0)
            {
                nextServerIndex += 1;
                return serverList[serverIndex];
            }else
            {
                return string.Empty;
            }
        }

        public int GetServerCount()
        {
            return serverList.Count;
        }
    }


}
