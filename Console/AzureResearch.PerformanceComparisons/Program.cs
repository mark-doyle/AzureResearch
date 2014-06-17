using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AzureResearch.PerformanceComparisons
{
    class Program
    {
        /*
         * Tables to pre-load:
         *  - Narrow Entity / Large Partition / Many Rows
         *  - Narrow Entity / Large Partition / Few Rows
         *  - Narrow Entity / Small Partition / Many Rows
         *  - Narrow Entity / Small Partition / Few Rows
         *  - Wide Entity / Large Partition / Many Rows
         *  - Wide Entity / Large Partition / Few Rows
         *  - Wide Entity / Small Partition / Many Rows
         *  - Wide Entity / Small Partition / Few Rows
         * 
         * Things to compare
         *  - Parallel versus serial retrieval (native)
         *  - Native versus Lokad retrieval
         *  - ITableEntity versus Customer (Data Service Key) retrieval
         *  - Narrow versus Wide retrieval
         */
        static void Main(string[] args)
        {
            int selection = DisplayMenuAndGetSelection();
            while (selection != 0)
            {
                selection = DisplayMenuAndGetSelection();

                switch (selection)
                {
                    case 1:
                        LoadData();
                        break;
                    default:
                        break;
                }
            }
        }

        static int DisplayMenuAndGetSelection()
        {
            string selection = null;
            int selectionParsed = -1;
            while (selectionParsed == -1)
            {
                Console.Clear();

                Console.WriteLine("*** Azure Research ***");
                Console.WriteLine("1 - Data load");
                Console.WriteLine("2 - Parallel versus serial retrieval");
                Console.WriteLine("3 - ITableEntity versus Customer (Data Service Key) retrieval");
                Console.WriteLine("4 - Narrow versus Wide retrieval");
                Console.WriteLine("0 - Exit");

                Console.Write("Enter selection: ");
                selection = Console.ReadLine();

                int.TryParse(selection, out selectionParsed);
            }
            return selectionParsed;
        }

        static void LoadData() 
        {
            DataLoader loader = new DataLoader();

            Console.WriteLine("Press ENTER to begin loading data");
            loader.LoadData();
            Console.WriteLine("Complete. Press ENTER");
        }
    }
}
