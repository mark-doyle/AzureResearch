using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using AzureResearch.Common.Entities;
using AzureResearch.Search.Lucene;

namespace AzureResearch.Search.IndexLoader
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Press ENTER to start index loader");
            Console.ReadLine();

            string connectionString = ConfigurationManager.AppSettings["AzureConnection"];
            CustomSearchTableRepository tableRepo = new CustomSearchTableRepository(connectionString);
            CustomSearchQueueRepository queueRepo = new CustomSearchQueueRepository(connectionString);

            int rowsToLoad = 100;
            int.TryParse(ConfigurationManager.AppSettings["RowsToLoad"], out rowsToLoad);
            if (rowsToLoad <= 0)
                rowsToLoad = 100;

            Console.WriteLine("Loading {0} entities", rowsToLoad);

            for (int iteration = 0; iteration < rowsToLoad; iteration++)
            {
                CustomSearchEntity entity = CustomSearchEntity.GetRandom();
                Parallel.Invoke(
                    () =>
                    {
                        tableRepo.InsertEntity(entity);
                    },
                    () =>
                    {
                        CustomSearchIndexingRequest request = new CustomSearchIndexingRequest()
                        {
                            DeIndex = false,
                            Entity = entity,
                            Optimize = false,
                            PurgeAll = false
                        };
                        queueRepo.QueueIndexingRequest(request);
                    }
                );
            }

            Console.WriteLine("Sending optimization request");

            queueRepo.QueueIndexingRequest(new CustomSearchIndexingRequest()
            {
                Optimize = true
            });

            Console.WriteLine("Press ENTER to stop index loader");
            Console.ReadLine();

        }
    }
}
