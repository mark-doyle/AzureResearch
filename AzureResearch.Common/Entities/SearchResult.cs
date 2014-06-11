using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage.Table;

namespace AzureResearch.Common.Entities
{
    public class SearchResult<T> where T : TableEntity
    {
        #region Declarations

        #endregion // Declarations

        #region Constructors

        #endregion // Constructors

        #region Private Methods

        #endregion // Private Methods

        #region Protected Methods

        #endregion // Protected Methods

        #region Public Methods

        #endregion // Public Methods

        #region Public Properties

        public int ResultCount { get; set; }
        public List<T> Results { get; set; }

        #endregion // Public Properties

    }
}
