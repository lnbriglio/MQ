using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MongoDB.Bson;
using MongoDB.Driver;
using MongoDB.Driver.Linq;
using MDS.Serialization;
using MongoDB.Bson.Serialization.Attributes;

namespace MDS.Storage.MongoDBStorage
{
    public class MongoDBStorageManager : IStorageManager
    {

        protected IMongoClient _client;
        protected IMongoDatabase _database;

        /// <summary>
        /// Table o collection name
        /// </summary>
        protected String _tableName = "messages";

        public MongoDBStorageManager()
        {
            _client = new MongoClient();
            _database = _client.GetDatabase("MDS");
        }

        
        public int StoreMessage(MDSMessageRecord messageRecord)
        {
            int id = 0;

            var bytesOfMessage = MDSSerializationHelper.SerializeToByteArray(messageRecord.Message);



            //Buscando id e insertando
            while (true)
            {
                var collection = _database.GetCollection<MDSMessageRecord>(_tableName);

                int lastId = collection.AsQueryable()
                    .OrderByDescending(m => m.Id)
                    .Select(m => m.Id)
                    .FirstOrDefault();

                id = lastId + 1;
                messageRecord.Id = id;

                try
                {
                    collection.InsertOne(messageRecord);

                }
                catch (Exception writeError)
                {
                    if (writeError.Message.Contains("11000"))
                        continue;
                    else
                        throw writeError;
                }

                break;
            }

            return id;
        }

        public List<MDSMessageRecord> GetWaitingMessagesOfApplication(string nextServer, string destApplication, int minId, int maxCount)
        {
            var collection = _database.GetCollection<MDSMessageRecord>(_tableName);

            return collection.AsQueryable()
                .OrderBy(m => m.Id)
                .Where(m => m.NextServer == nextServer &&
                m.DestApplication == destApplication &&
                m.Id >= minId)
                .Take(maxCount)
                .ToList<MDSMessageRecord>();

        }

        public int GetMaxWaitingMessageIdOfApplication(string nextServer, string destApplication)
        {
            var collection = _database.GetCollection<MDSMessageRecord>(_tableName);

            return collection.AsQueryable()
                .OrderByDescending(m => m.Id)
                .Where(m => m.NextServer == nextServer &&
                m.DestApplication == destApplication)
                .Select(m => m.Id)
                .FirstOrDefault();
        }

        public List<MDSMessageRecord> GetWaitingMessagesOfServer(string nextServer, int minId, int maxCount)
        {
            var collection = _database.GetCollection<MDSMessageRecord>(_tableName);

            return collection.AsQueryable()
                .OrderBy(m => m.Id)
                .Where(m => m.NextServer == nextServer &&
                m.Id >= minId)
                .Take(maxCount)
                .ToList<MDSMessageRecord>();
        }

        public int GetMaxWaitingMessageIdOfServer(string nextServer)
        {
            var collection = _database.GetCollection<MDSMessageRecord>(_tableName);

            return collection.AsQueryable()
                .OrderByDescending(m => m.Id)
                .Where(m => m.NextServer == nextServer)
                .Select(m => m.Id)
                .FirstOrDefault();
        }

        public int RemoveMessage(int id)
        {
            var collection = _database.GetCollection<MDSMessageRecord>(_tableName);
            var filter = Builders<MDSMessageRecord>.Filter.Eq("_id", id);
            var result = collection.DeleteMany(filter);

            return (int)result.DeletedCount;
        }

        public void UpdateNextServer(string destServer, string nextServer)
        {
            var collection = _database.GetCollection<MDSMessageRecord>(_tableName);
            var filter = Builders<MDSMessageRecord>.Filter.Eq("DestServer", destServer);
            var update = Builders<MDSMessageRecord>.Update.Set("NextServer", nextServer);

            var result = collection.UpdateMany(filter, update);
        }

        public void Start()
        {
            //No Action
        }

        public void Stop(bool waitToStop)
        {
            //No Action
        }

        public void WaitToStop()
        {
            //No action
        }


    }

    /*
    public class MessageBson
        {
        [BsonId]
        private int _id { get; set; }
                            private string message_id {get;set;}
            private string dest_server {get;set;}
            private string next_server {get;set;}
            private string dest_application { get; set; }
            private byte[] message_data {get;set;}
            private string message_data_length {get;set;}

            private void MessageBson(MDSMessageRecord record)
            {
                bytesArray

                message_id = record.MessageId;
                dest_server = record.DestServer;
                next_server = record.DestServer;
                dest_application = record.DestApplication;
                message_data = MDSSerializationHelper.SerializeToByteArray(record.Message);
                message_data_length = 
            }

            private MDSMessageRecord ToMDSMessageRecord()
            {

            }
        }
     * */
}
