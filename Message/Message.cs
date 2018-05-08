using System;
using System.Runtime.Serialization;

namespace MasterSlaveController
{
    // Enumeration that define the type of message
    public enum MessageType { operation, DB, Info }

    [Serializable]
    public class Message
    {
        private MessageType _messageType;

        private byte[] _data { get; set; }

        public Message(byte[] data, MessageType type)
        {

            this._data = data;
            this.Type = type;
        }
        public Message()
        {
        }

        public MessageType Type
        {
            get
            {
                return this._messageType;
            }
            set
            {
                this._messageType = value;
            }
        }


        public byte[] Data
        {
            get
            {
                return this._data;
            }
            set
            {
                this._data = value;
            }
        }
    }
}