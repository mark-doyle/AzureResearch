using Lokad.Cloud.Storage;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;

namespace AzureResearch.Common.Repositories
{
    public class LokadTableStorageRepository<T> : ITableStorageRepository<T> where T : class, ITableEntity, new()
    {

        // Name of table where BaseEntities are
        protected readonly string TableName;

        // Lokad storage provider
        protected readonly ITableStorageProvider TableStorageProvider;

        // Storage account
        protected readonly CloudStorageAccount StorageAccount;

        #region Internal constructors

        /// <summary>
        /// Constructor that takes a different storage account
        /// </summary>
        /// <param name="tableName"></param>
        /// <param name="storageAccountConfigKey"></param>
        [ExcludeFromCodeCoverage]
        internal LokadTableStorageRepository(string tableName, string connectionString = "")
        {
            if (string.IsNullOrEmpty(connectionString))
                StorageAccount = CloudStorageAccount.DevelopmentStorageAccount;
            else
                StorageAccount = CloudStorageAccount.Parse(connectionString);

            // Set the table name
            TableName = tableName;

            // Create the table provider
            TableStorageProvider = CloudStorage.ForAzureAccount(StorageAccount).BuildTableStorage();
        }

        /// <summary>
        /// This constructor is for unit testing. It injects all of the parameters so they can be mocked.
        /// </summary>
        /// <param name="tableName"></param>
        /// <param name="tableStorageProvider"></param>
        internal LokadTableStorageRepository(string tableName, ITableStorageProvider tableStorageProvider)
        {
            // Inject all of properties
            TableName = tableName;
            TableStorageProvider = tableStorageProvider;
        }

        #endregion

        #region Protected Methods

        /// <summary>
        /// Transform BaseEntities into CloudEntities so Lokad will 
        /// know how to use them.
        /// </summary>
        /// <param name="entities">BaseEntities to transform</param>
        /// <returns>List of CloudEntities</returns>
        protected IEnumerable<CloudEntity<T>> GetCloudEntities(IEnumerable<T> entities)
        {
            var cloudEntities = entities.Select(baseEntity => new CloudEntity<T>
            {
                PartitionKey = baseEntity.PartitionKey,
                RowKey = baseEntity.RowKey,
                Value = baseEntity
            });

            return cloudEntities;
        }

        /// <summary>
        /// Determines whether or not to use the Dev Storage or Cloud Storage 
        /// strategy for upserting
        /// </summary>
        /// <param name="cloudEntities"></param>
        [ExcludeFromCodeCoverage]
        protected void InternalUpsert(IEnumerable<CloudEntity<T>> cloudEntities)
        {
            // Make sure to not update items that don't need updating
            var enititiesToUpload = cloudEntities;

            // Upsert all of the lists to table storage
            TableStorageProvider.Upsert(TableName, enititiesToUpload);
        }

        #endregion

        #region Public methods

        #region Get methods

        /// <summary>
        /// Get the BaseEntity with the rowKey and partitionKey specified in 
        /// the parameters
        /// </summary>
        /// <param name="partitionKey">The partitionKey of the BaseEntity</param>
        /// <param name="rowKey">The rowKey of the BaseEntity</param>
        /// <returns>The BaseEntity</returns>
        public virtual T GetEntity(string partitionKey, string rowKey)
        {
            // Ensure the arguments exist
            if (string.IsNullOrEmpty(rowKey))
            {
                throw new ArgumentNullException(String.Format("Null rowKey in table: {0}", TableName));
            }

            if (string.IsNullOrEmpty(partitionKey))
            {
                throw new ArgumentNullException(String.Format("Null partitionKey in table: {0}", TableName));
            }

            //var rowKeys = new List<string> {rowKey};
            //var entityEnumerable = GetEntities(partitionKey, rowKeys);
            //return entityEnumerable.FirstOrDefault();

            var cloudEntity = TableStorageProvider.Get<T>(TableName, partitionKey, rowKey);
            return cloudEntity.HasValue ? cloudEntity.Value.Value : default(T);
        }

        /// <summary>
        /// Get all of the BaseEntities in the specified partition
        /// </summary>
        /// <param name="partitionKey">The partitionKey of the BaseEntity</param>
        /// <returns>The BaseEntities in the partition</returns>
        public virtual IEnumerable<T> GetEntities(string partitionKey)
        {
            if (string.IsNullOrEmpty(partitionKey))
            {
                throw new ArgumentNullException(String.Format("Null partitionKey in table: {0}", TableName));
            }
            return TableStorageProvider.Get<T>(TableName, partitionKey).Select(e => e.Value);
        }

        /// <summary>
        /// Get all of the entities with one of the RowKeys and the partionKey specified
        /// in the parameters
        /// </summary>
        /// <param name="partitionKey">The Partitionkey of the BaseEntities</param>
        /// <param name="rowKeys">The RowKeys of the BaseEntities</param>
        /// <returns>BaseEntities from the partition</returns>
        public virtual IEnumerable<T> GetEntities(string partitionKey, IEnumerable<string> rowKeys)
        {
            if (string.IsNullOrEmpty(partitionKey))
            {
                throw new ArgumentNullException(String.Format("Null partitionKey: {0}", partitionKey));
            }

            if (rowKeys == null)
            {
                throw new ArgumentNullException("Null Row key list");
            }

            return TableStorageProvider.Get<T>(TableName, partitionKey, rowKeys).Select(e => e.Value);
        }

        /// <summary>
        /// Get all of the entities with one of the RowKeys and the partionKey specified
        /// in the parameters
        /// </summary>
        /// <param name="partitionKey">The Partitionkey of the BaseEntities</param>
        /// <param name="minRowKey"></param>
        /// <param name="maxRowKey"></param>
        /// <returns>BaseEntities from the partition</returns>
        public virtual IEnumerable<T> GetEntities(string partitionKey, string minRowKey, string maxRowKey)
        {
            if (string.IsNullOrEmpty(partitionKey))
            {
                throw new ArgumentNullException(String.Format("Null partitionKey: {0}", partitionKey));
            }

            if (string.IsNullOrEmpty(minRowKey))
            {
                throw new ArgumentNullException("Null min row key");
            }

            if (string.IsNullOrEmpty(maxRowKey))
            {
                throw new ArgumentNullException("Null max row key");
            }

            return TableStorageProvider.Get<T>(TableName, partitionKey, minRowKey, maxRowKey).Select(e => e.Value);
        }

        /// <summary>
        /// Get all of the BaseEntities from table storage
        /// </summary>
        /// <returns>All of the BaseEntities in table storage</returns>
        public virtual IEnumerable<T> GetAllEntities()
        {
            return TableStorageProvider.Get<T>(TableName).Select(e => e.Value);
        }

        #endregion

        #region Insert methods

        /// <summary>
        /// Insert the BaseEntity into table storage
        /// </summary>
        /// <param name="entity">The BaseEntity to insert</param>
        public virtual void InsertEntity(T entity)
        {
            // Create a list of entities, so we can call InsertEntities. 
            // Lokad requires an IEnumerable to Insert items
            var entities = new List<T> { entity };
            InsertBatch(entities);
        }

        /// <summary>
        /// Insert the BaseEntities into table storage
        /// </summary>
        /// <param name="entities">The BaseEntities to insert</param>
        public virtual void InsertBatch(IEnumerable<T> entities)
        {
            if (entities != null)
            {
                var cloudEntities = GetCloudEntities(entities);
                TableStorageProvider.Insert(TableName, cloudEntities);
            }
        }

        #endregion

        #region Update methods
        /// <summary>
        /// Insert the BaseEntity into table storage
        /// </summary>
        /// <param name="entity">The BaseEntity to insert</param>
        public virtual void UpdateEntity(T entity)
        {
            // Create a list of entities, so we can call InsertEntities. 
            // Lokad requires an IEnumerable to Insert items
            var entities = new List<T> { entity };
            UpdateBatch(entities);
        }

        /// <summary>
        /// Insert the BaseEntities into table storage
        /// </summary>
        /// <param name="entities">The BaseEntities to insert</param>
        public virtual void UpdateBatch(IEnumerable<T> entities)
        {
            var cloudEntities = GetCloudEntities(entities);
            TableStorageProvider.Update(TableName, cloudEntities, true);
        }

        #endregion

        #region Upsert methods

        /// <summary>
        /// Upsert the BaseEntity into table storage
        /// </summary>
        /// <param name="entity">The BaseEntity to upsert</param>
        public virtual void UpsertEntity(T entity)
        {
            // Create a list of entities, so we can call UpsertEntities. 
            // Lokad requires an IEnumerable to Upsert items
            var entities = new List<T> { entity };
            UpsertBatch(entities);
        }
        /// <summary>
        /// Upsert the provided IEnumerable of Base entities into
        /// table storage
        /// </summary>
        /// <param name="entities">The BaseEntities to upsert</param>
        public virtual void UpsertBatch(IEnumerable<T> entities)
        {
            var cloudEntities = GetCloudEntities(entities);
            InternalUpsert(cloudEntities);
        }

        #endregion

        #region Delete methods

        /// <summary>
        /// Deletes the entity from table.
        /// </summary>
        /// <param name="entity">BaseEntity to delete</param>
        public virtual void DeleteEntity(T entity)
        {
            // Create a list of entities, so we can call Delete Entities. 
            // Lokad requires an IEnumerable to Delete items
            var entities = new List<T> { entity };
            DeleteBatch(entities);
        }

        /// <summary>
        /// Deletes the specified entities from the table
        /// </summary>
        /// <param name="entities">BaseEntity to delete</param>
        public virtual void DeleteBatch(IEnumerable<T> entities)
        {
            var cloudEntities = GetCloudEntities(entities);
            TableStorageProvider.Delete(TableName, cloudEntities, true);
        }

        /// <summary>
        /// Deletes the specified entities from the table
        /// </summary>
        /// <param name="partitionKey"></param>
        public virtual void DeletePartition(string partitionKey)
        {
            if (string.IsNullOrEmpty(partitionKey))
            {
                throw new ArgumentNullException("Null partitionKey");
            }

            var partitionEntities = TableStorageProvider.Get<T>(TableName, partitionKey);

            TableStorageProvider.Delete(TableName, partitionEntities, true);
        }

        #endregion

        #endregion

    }
}
