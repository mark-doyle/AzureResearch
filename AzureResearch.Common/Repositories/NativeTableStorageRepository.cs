using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using Microsoft.WindowsAzure.Storage.Table.DataServices;
using System;
using System.Collections.Generic;
using System.Data.Services.Client;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;

namespace AzureResearch.Common.Repositories
{
    public class NativeTableStorageRepository<T> : ITableStorageRepository<T> where T : TableEntity
    {
        #region Declarations

        protected readonly CloudStorageAccount _cloudStorageAccount;
        protected TableServiceContext _tableContext;
        protected string _tableName;
        protected CloudTable _cloudTable;

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

            CloudTableClient tableClient = _cloudStorageAccount.CreateCloudTableClient();
            _cloudTable = tableClient.GetTableReference(_tableName);
            _cloudTable.CreateIfNotExists();
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
                                    batchOperation.Replace(item);
                                    break;
                                case BatchOperationType.Upsert:
                                    batchOperation.InsertOrReplace(item);
                                    break;
                                case BatchOperationType.Delete:
                                    item.ETag = "*";
                                    batchOperation.Delete(item);
                                    break;
                            }
                            actualResults++;
                        }

                        _cloudTable.ExecuteBatch(batchOperation);
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
            var query =
                (from entity in TableServiceContext.CreateQuery<T>(_tableName)
                 where entity.PartitionKey == partitionKey && entity.RowKey == rowKey
                 select entity).AsTableServiceQuery(TableServiceContext);

            return query.Execute().FirstOrDefault();
        }

        public virtual IEnumerable<T> GetEntities(string partitionKey)
        {
            return
                (from entity in TableServiceContext.CreateQuery<T>(_tableName)
                 where entity.PartitionKey == partitionKey
                 select entity).AsTableServiceQuery(TableServiceContext);
        }

        public virtual IEnumerable<T> GetEntities(string partitionKey, IEnumerable<string> rowKeys)
        {
            List<T> results = new List<T>();

            List<Task<T>> tasks = (from rowKey in rowKeys
                                   select Task<T>.Factory.StartNew(() => GetEntity(partitionKey, rowKey))).ToList();
            Task.WaitAll(tasks.ToArray());
            tasks.ForEach(task => results.Add(task.Result));

            return results;
        }

        public virtual IEnumerable<T> GetEntities(string partitionKey, string minRowKey, string maxRowKey)
        {
            return
                (from entity in TableServiceContext.CreateQuery<T>(_tableName)
                 where entity.PartitionKey == partitionKey && entity.RowKey.CompareTo(minRowKey) >= 0 && entity.RowKey.CompareTo(maxRowKey) <= 0
                 select entity).AsTableServiceQuery(TableServiceContext);
        }

        public virtual IEnumerable<T> GetWhere(Expression<Func<T, bool>> expression)
        {
            var query = this.TableServiceContext.CreateQuery<T>(this._tableName);
            return query.Where(expression);
        }

        public virtual IEnumerable<T> GetAllEntities()
        {
            return
                (from entity in TableServiceContext.CreateQuery<T>(_tableName)
                 select entity).AsTableServiceQuery(TableServiceContext);
        }

        public virtual void InsertEntity(T entity)
        {
            TableOperation insertOperation = TableOperation.Insert(entity);
            _cloudTable.Execute(insertOperation);
        }

        public virtual void InsertBatch(IEnumerable<T> entities)
        {
            _BatchOperation(entities, BatchOperationType.Insert);
        }

        public virtual void UpdateEntity(T entity)
        {
            TableOperation replaceOperation = TableOperation.Replace(entity);
            _cloudTable.Execute(replaceOperation);
        }

        public virtual void UpdateBatch(IEnumerable<T> entities)
        {
            _BatchOperation(entities, BatchOperationType.Update);
        }

        public virtual void UpsertEntity(T entity)
        {
            TableOperation upsertOperation = TableOperation.InsertOrReplace(entity);
            _cloudTable.Execute(upsertOperation);
        }

        public virtual void UpsertBatch(IEnumerable<T> entities)
        {
            _BatchOperation(entities, BatchOperationType.Upsert);
        }

        public virtual void DeleteEntity(T entity)
        {
            TableOperation deleteOperation = TableOperation.Delete(entity);
            _cloudTable.Execute(deleteOperation);
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
