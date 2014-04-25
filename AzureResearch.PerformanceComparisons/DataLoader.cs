using AzureResearch.Common.Constants;
using AzureResearch.Common.Entities;
using AzureResearch.Common.Repositories;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AzureResearch.PerformanceComparisons
{
    public class DataLoader
    {
        private string _tableStorageConnectionString;

        public void LoadData()
        {
            _tableStorageConnectionString = ConfigurationManager.AppSettings["AzureStorage"];
        }

        private void NativeLoadNarrowEntityLargePartitionManyRows()
        {
            // Storage: Native
            // Partition: Year
            // Row: Date to minutes
            // Data: 10 years

            var repo = new NativeTableStorageRepository<NarrowEntity>(_tableStorageConnectionString, TableNames.NativeNarrowEntityLargePartitionManyRows);
            List<NarrowEntity> entities = new List<NarrowEntity>();

            DateTime current = DateTime.Parse("2000-01-01");
            DateTime end = DateTime.Parse("2010-01-01");
            while (current < end)
            {
                current = current.AddMinutes(1);
            }

        }

        private void NativeLoadNarrowEntityLargePartitionFewRows()
        {
            // Storage: Native
            // Partition: Year
            // Row: Date to hours
            // Data: 10 years
        }

        private void NativeLoadNarrowEntitySmallPartitionManyRows()
        {
            // Storage: Native
            // Partition: Date (day)
            // Row: Date to minutes
            // Data: 10 years
        }

        private void NativeLoadNarrowEntitySmallPartitionFewRows()
        {
            // Storage: Native
            // Partition: Date (day)
            // Row: Date to hours
            // Data: 10 years
        }

        private void NativeLoadWideEntityLargePartitionManyRows()
        {
            // Storage: Native
            // Partition: Year
            // Row: Date to seconds
            // Data: 10 years
        }

        private void NativeLoadWideEntityLargePartitionFewRows()
        {
            // Storage: Native
            // Partition: Year
            // Row: Date to hours
            // Data: 10 years
        }

        private void NativeLoadWideEntitySmallPartitionManyRows()
        {
            // Storage: Native
            // Partition: Date (day)
            // Row: Date to seconds
            // Data: 10 years
        }

        private void NativeLoadWideEntitySmallPartitionFewRows()
        {
            // Storage: Native
            // Partition: Date (day)
            // Row: Date to hours
            // Data: 10 years
        }

        private void LokadLoadNarrowEntityLargePartitionManyRows()
        {
            // Storage: Lokad
            // Partition: Year
            // Row: Date to minutes
            // Data: 10 years
        }

        private void LokadLoadNarrowEntityLargePartitionFewRows()
        {
            // Storage: Lokad
            // Partition: Year
            // Row: Date to hours
            // Data: 10 years
        }

        private void LokadLoadNarrowEntitySmallPartitionManyRows()
        {
            // Storage: Lokad
            // Partition: Date (day)
            // Row: Date to minutes
            // Data: 10 years
        }

        private void LokadLoadNarrowEntitySmallPartitionFewRows()
        {
            // Storage: Lokad
            // Partition: Date (day)
            // Row: Date to hours
            // Data: 10 years
        }

        private void LokadLoadWideEntityLargePartitionManyRows()
        {
            // Storage: Lokad
            // Partition: Year
            // Row: Date to seconds
            // Data: 10 years
        }

        private void LokadLoadWideEntityLargePartitionFewRows()
        {
            // Storage: Lokad
            // Partition: Year
            // Row: Date to hours
            // Data: 10 years
        }

        private void LokadLoadWideEntitySmallPartitionManyRows()
        {
            // Storage: Lokad
            // Partition: Date (day)
            // Row: Date to minutes
            // Data: 10 years
        }

        private void LokadLoadWideEntitySmallPartitionFewRows()
        {
            // Storage: Lokad
            // Partition: Date (day)
            // Row: Date to hours
            // Data: 10 years
        }

    }
}
