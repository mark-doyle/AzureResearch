using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using AzureResearch.Common.Constants;
using AzureResearch.Common.Entities;
using AzureResearch.Common.Repositories;

namespace AzureResearch.Search.Lucene
{
    public class CustomSearchTableRepository : NativeTableStorageRepository<CustomSearchEntity>
    {
        #region Declarations

        #endregion // Declarations

        #region Constructors

        public CustomSearchTableRepository(string connectionString)
            : base(connectionString, TableNames.CustomSearchEntity)
        {
        }

        #endregion // Constructors

        #region Private Methods

        #endregion // Private Methods

        #region Protected Methods

        #endregion // Protected Methods

        #region Public Methods

        #endregion // Public Methods

        #region Public Properties

        #endregion // Public Properties

    }
}
