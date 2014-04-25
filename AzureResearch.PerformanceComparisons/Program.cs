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
        }
    }
}
