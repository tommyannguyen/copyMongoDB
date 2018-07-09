using MongoDB.Bson;
using MongoDB.Driver;
using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;

namespace ImportMongoDB
{
    class Program
    {
        static async Task Main(string[] args)
        {
          
            var localDatabase = new MongoClient("mongodb://localhost:27017").GetDatabase("customerarea");

            var azureDatabase = new MongoClient("mongodb://localhost:27017").GetDatabase("coredb");

            var collectionNames = new List<string> { "AccountAndUser","CommonData","Part","Quote","UserData" };

            foreach (var collectionName in collectionNames)
            {
                var fileName = collectionName + ".json";

                Console.WriteLine("Copy collection :" + fileName);
                await WriteCollectionToFile(azureDatabase, collectionName, fileName);

                await localDatabase.DropCollectionAsync(collectionName);

                await LoadCollectionFromFile(localDatabase, collectionName, fileName);
                Console.WriteLine("Done collection :" + fileName);
            }

            Console.WriteLine("Done !");
            Console.WriteLine("Hello World!");
            Console.Read();
        }

        public static async Task WriteCollectionToFile(IMongoDatabase database, string collectionName, string fileName)
        {
            var collection = database.GetCollection<RawBsonDocument>(collectionName);

            // Make sure the file is empty before we start writing to it
            File.WriteAllText(fileName, string.Empty);

            using (var cursor = await collection.FindAsync(new BsonDocument()))
            {
                while (await cursor.MoveNextAsync())
                {
                    var batch = cursor.Current;
                    foreach (var document in batch)
                    {
                        File.AppendAllLines(fileName, new[] { document.ToString() });
                    }
                }
            }
        }

        public static async Task LoadCollectionFromFile(IMongoDatabase database, string collectionName, string fileName)
        {
            using (FileStream fs = File.Open(fileName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
            using (BufferedStream bs = new BufferedStream(fs))
            using (StreamReader sr = new StreamReader(bs))
            {
                var collection = database.GetCollection<BsonDocument>(collectionName);

                string line;
                while ((line = sr.ReadLine()) != null)
                {
                    await collection.InsertOneAsync(BsonDocument.Parse(line));
                }
            }
        }
    }
}
