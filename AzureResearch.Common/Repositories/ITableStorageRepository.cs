using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;

namespace AzureResearch.Common.Repositories
{
    public interface ITableStorageRepository<T>
    {
        // Get entities
        T GetEntity(string partitionKey, string rowKey);
        IEnumerable<T> GetEntities(string partitionKey);
        IEnumerable<T> GetEntities(string partitionKey, IEnumerable<string> rowKeys);
        IEnumerable<T> GetEntities(string partitionKey, string minRowKey, string maxRowKey);
        IEnumerable<T> GetWhere(Expression<Func<T, bool>> expression);
        IEnumerable<T> GetAllEntities();

        // Insert entity
        void InsertEntity(T entity);
        void InsertBatch(IEnumerable<T> entities);

        // Update entity
        void UpdateEntity(T entity);
        void UpdateBatch(IEnumerable<T> entities);

        // Upsert entity
        void UpsertEntity(T entity);
        void UpsertBatch(IEnumerable<T> entities);

        // Delete entities
        void DeleteEntity(T entity);
        void DeleteBatch(IEnumerable<T> entities);
        void DeletePartition(string partitionKey);

    }
}
