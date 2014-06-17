using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AzureResearch.Common.Entities
{
    public class CustomSearchIndexingRequest
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

        public CustomSearchEntity Entity { get; set; }

        public bool DeIndex { get; set; }

        public bool Optimize { get; set; }

        public bool PurgeAll { get; set; }

        #endregion // Public Properties

    }
}
