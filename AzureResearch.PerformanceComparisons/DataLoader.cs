using AzureResearch.Common.Constants;
using AzureResearch.Common.Entities;
using AzureResearch.Common.Repositories;
using AzureResearch.PerformanceComparisons.Properties;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AzureResearch.PerformanceComparisons
{
    public class DataLoader
    {
        private string _tableStorageConnectionString;
        private int _lipsumTextLength = 0;
        private Random _random;

        public void LoadData()
        {
            _tableStorageConnectionString = ConfigurationManager.AppSettings["AzureStorage"];

            _lipsumTextLength = Resources.LoremIpsum.Length;
            _random = new Random();

            NativeLoadNarrowEntityLargePartitionManyRows();
        }

        private void _PopulateEntity(NarrowEntity entity)
        {
            // fields
            entity.Field01 = Resources.LoremIpsum.Substring(0, _random.Next(_lipsumTextLength));
            entity.Field02 = Resources.LoremIpsum.Substring(0, _random.Next(_lipsumTextLength));
            entity.Field03 = Resources.LoremIpsum.Substring(0, _random.Next(_lipsumTextLength));
            entity.Field04 = Resources.LoremIpsum.Substring(0, _random.Next(_lipsumTextLength));
            entity.Field05 = Resources.LoremIpsum.Substring(0, _random.Next(_lipsumTextLength));

            if (entity is WideEntity)
            {
                var wide = (WideEntity)entity;
                wide.Field06 = Resources.LoremIpsum.Substring(0, _random.Next(_lipsumTextLength));

            }
        }

        private void NativeLoadNarrowEntityLargePartitionManyRows()
        {
            // Storage: Native
            // Partition: Year
            // Row: Date to minutes
            // Data: 10 years

            var repo = new NativeTableStorageRepository<NarrowEntity>(_tableStorageConnectionString, TableNames.NativeNarrowEntityLargePartitionManyRows);
            List<NarrowEntity> entities = new List<NarrowEntity>();

            int totalEntities = 0;
            DateTime current = DateTime.Parse("2000-01-01");
            DateTime end = DateTime.Parse("2010-01-01");
            //while (current < end)
            //{
            //    current = current.AddMinutes(1);

            //    var entity = new NarrowEntity()
            //    {
            //        PartitionKey = current.Year.ToString(),
            //        RowKey = current.ToString("yyyyMMddHHmm")
            //    };
            //    _PopulateEntity(entity);
            //    entities.Add(entity);
            //    totalEntities++;

            //    if (entities.Count % 1000 == 0)
            //    {
            //        // save & clear list
            //        //repo.UpdateBatch(entities);
            //        entities.Clear();
            //        Debug.WriteLine("Entities added: " + totalEntities.ToString());
            //    }
            //}

            Debug.WriteLine("Total entities added: " + totalEntities.ToString());
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
