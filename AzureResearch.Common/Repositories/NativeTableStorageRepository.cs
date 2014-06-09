using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using Microsoft.WindowsAzure.Storage.Table.DataServices;
using Microsoft.WindowsAzure.Storage.Table.Protocol;
using System;
using System.Collections.Generic;
using System.Data.Services.Client;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;

namespace AzureResearch.Common.Repositories
{
    public class NativeTableStorageRepository<T> : ITableStorageRepository<T> where T : class, ITableEntity, new()
    {
        #region Declarations

        protected readonly CloudStorageAccount _cloudStorageAccount;
        protected TableServiceContext _tableContext;
        protected string _tableName;
        protected CloudTableClient _tableClient;

        #endregion // Declarations

        #region Properties

        /// <summary>
        /// Gets the TableserviceContext for the current storage account
        /// </summary>
        /// <returns></returns>
        protected TableServiceContext TableServiceContext
        {
            get
            {
                if (_tableContext == null)
                    _tableContext = new TableServiceContext(new CloudTableClient(_cloudStorageAccount.TableEndpoint, _cloudStorageAccount.Credentials));
                return _tableContext;
            }
        }

        #endregion // Properties

        #region Constructors

        /// <summary>
        /// Default contructor that uses the storage account and credentials in configuration
        /// </summary>
        /// <param name="tableName">Table name to be used in storage operations</param>
        public NativeTableStorageRepository(string tableName)
            : this(CloudStorageAccount.DevelopmentStorageAccount, tableName)
        {
        }

        /// <summary>
        /// Constructor that uses a given storage account name
        /// </summary>
        /// <param name="tableName">Table name to be used in storage operations</param>
        /// <param name="connectionString">Table storage connection string</param>
        public NativeTableStorageRepository(string connectionString, string tableName)
            : this(CloudStorageAccount.Parse(connectionString), tableName)
        {

        }

        /// <summary>
        /// Injectable constructor 
        /// </summary>
        /// <param name="cloudStorageAccount">CloudStorageAccount to use</param>
        /// <param name="tableName">Table name to be used in storage operations</param>
        public NativeTableStorageRepository(CloudStorageAccount cloudStorageAccount, string tableName)
        {
            if (string.IsNullOrEmpty(tableName))
            {
                throw new ArgumentNullException("table name");
            }

            // create storage account
            _cloudStorageAccount = cloudStorageAccount;
            _tableName = tableName;

            _tableClient = _cloudStorageAccount.CreateCloudTableClient();
            var cloudTable = _tableClient.GetTableReference(_tableName);
            cloudTable.CreateIfNotExists();
        }

        #endregion // Constructors

        #region Private Methods

        /// <summary>
        /// Executes a batch table storage operation
        /// </summary>
        /// <param name="entities">List of entities used in the operation</param>
        /// <param name="operationType">Batch operation type (private enum <see cref="BatchOperationType"/> declared in the NativeTableStorageRepository class)</param>
        private void _BatchOperation(IEnumerable<T> entities, BatchOperationType operationType)
        {
            var partitions = entities.GroupBy(entity => entity.PartitionKey);
            foreach (var partitionGroup in partitions)
            {
                IEnumerable<T> batch = null;
                int pass = 0;
                int actualResults = 0;
                int batchSize = 100;
                var cloudTable = _tableClient.GetTableReference(_tableName);

                do
                {
                    actualResults = 0;
                    int skip = pass * batchSize;
                    batch = partitionGroup.Skip(skip).Take(batchSize);
                    if (batch.Any())
                    {
                        TableBatchOperation batchOperation = new TableBatchOperation();

                        foreach (var item in batch)
                        {
                            switch (operationType)
                            {
                                case BatchOperationType.Insert:
                                    batchOperation.Insert(item);
                                    break;
                                case BatchOperationType.Update:
                                    item.ETag = "*";
                                    batchOperation.Replace(item);
                                    break;
                                case BatchOperationType.Upsert:
                                    item.ETag = "*";
                                    batchOperation.InsertOrReplace(item);
                                    break;
                                case BatchOperationType.Delete:
                                    item.ETag = "*";
                                    batchOperation.Delete(item);
                                    break;
                            }
                            actualResults++;
                        }

                        cloudTable.ExecuteBatch(batchOperation);
                    }
                    pass++;
                }
                while (actualResults == batchSize);
            }
        }

        #endregion // Private Methods

        #region Public Methods

        public virtual T GetEntity(string partitionKey, string rowKey)
        {
            var cloudTable = _tableClient.GetTableReference(_tableName);
            var query = TableOperation.Retrieve<T>(partitionKey, rowKey);
            var result = cloudTable.Execute(query).Result;

            return result as T;
        }

        public virtual IEnumerable<T> GetEntities(string partitionKey)
        {
            var cloudTable = _tableClient.GetTableReference(_tableName);
            var partitionKeyFilter = TableQuery.GenerateFilterCondition(TableConstants.PartitionKey, QueryComparisons.Equal, partitionKey);
            var query = new TableQuery<T>().Where(partitionKeyFilter);
            var entities = cloudTable.ExecuteQuery(query).ToList();

            return entities;
        }

        public virtual IEnumerable<T> GetEntities(string partitionKey, IEnumerable<string> rowKeys)
        {
            var results = new List<T>();

            var tasks = (from rowKey in rowKeys
                         select Task<T>.Factory.StartNew(() => GetEntity(partitionKey, rowKey))).ToList();
            Task.WaitAll(tasks.Cast<Task>().ToArray());
            tasks.ForEach(task => results.Add(task.Result));

            return results;
        }

        public virtual IEnumerable<T> GetEntities(string partitionKey, string minRowKey, string maxRowKey)
        {
            var cloudTable = _tableClient.GetTableReference(_tableName);
            var partitionFilter = TableQuery.GenerateFilterCondition(TableConstants.PartitionKey, QueryComparisons.Equal, partitionKey);
            var minFilter = TableQuery.GenerateFilterCondition(TableConstants.RowKey, QueryComparisons.GreaterThanOrEqual, minRowKey);
            var maxFilter = TableQuery.GenerateFilterCondition(TableConstants.RowKey, QueryComparisons.LessThanOrEqual, maxRowKey);

            var combinedRowFilter = TableQuery.CombineFilters(minFilter, TableOperators.And, maxFilter);
            var combinedFilter = TableQuery.CombineFilters(partitionFilter, TableOperators.And, combinedRowFilter);

            var query = new TableQuery<T>().Where(combinedFilter);
            var entities = cloudTable.ExecuteQuery(query).ToList();

            return entities;
        }

        public virtual IEnumerable<T> GetAllEntities()
        {
            var cloudTable = _tableClient.GetTableReference(_tableName);
            var query = cloudTable.CreateQuery<T>();
            var results = cloudTable.ExecuteQuery(query).ToList();

            return results;
        }

        public virtual void InsertEntity(T entity)
        {
            var cloudTable = _tableClient.GetTableReference(_tableName);
            TableOperation insertOperation = TableOperation.Insert(entity);
            cloudTable.Execute(insertOperation);
        }

        public virtual void InsertBatch(IEnumerable<T> entities)
        {
            _BatchOperation(entities, BatchOperationType.Insert);
        }

        public virtual void UpdateEntity(T entity)
        {
            var cloudTable = _tableClient.GetTableReference(_tableName);
            TableOperation replaceOperation = TableOperation.Replace(entity);
            cloudTable.Execute(replaceOperation);
        }

        public virtual void UpdateBatch(IEnumerable<T> entities)
        {
            _BatchOperation(entities, BatchOperationType.Update);
        }

        public virtual void UpsertEntity(T entity)
        {
            var cloudTable = _tableClient.GetTableReference(_tableName);
            TableOperation upsertOperation = TableOperation.InsertOrReplace(entity);
            cloudTable.Execute(upsertOperation);
        }

        public virtual void UpsertBatch(IEnumerable<T> entities)
        {
            _BatchOperation(entities, BatchOperationType.Upsert);
        }

        public virtual void DeleteEntity(T entity)
        {
            var cloudTable = _tableClient.GetTableReference(_tableName);
            TableOperation deleteOperation = TableOperation.Delete(entity);
            cloudTable.Execute(deleteOperation);
        }

        public virtual void DeleteBatch(IEnumerable<T> entities)
        {
            _BatchOperation(entities, BatchOperationType.Delete);
        }

        public virtual void DeletePartition(string partitionKey)
        {
            var targets = GetEntities(partitionKey);
            if (targets != null && targets.Any())
            {
                DeleteBatch(targets);
            }
        }

        #endregion // Public Methods

        #region Enums

        enum BatchOperationType
        {
            Insert,
            Update,
            Upsert,
            Delete
        }

        #endregion // Enums
    }
}
