using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using AzureResearch.Search.Lucene;

namespace AzureResearch.Search.IndexingWorker
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Press ENTER to start indexing worker");
            Console.ReadLine();

            SearchIndexingWorkerRole role = new SearchIndexingWorkerRole();
            role.OnStart();
            Task runTask = Task.Factory.StartNew(() => role.Run(), TaskCreationOptions.LongRunning);

            Console.WriteLine("Press ENTER to stop indexing worker");
            Console.ReadLine();

            role.OnStop();
            runTask.Wait();
            role = null;

        }
    }
}
