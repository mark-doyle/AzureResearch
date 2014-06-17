using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Serialization;
using AzureResearch.Common.Entities;
using AzureResearch.Common.Repositories;

namespace AzureResearch.Search.Lucene
{
    public class SearchIndexingWorker
    {
        #region Declarations

        private CustomSearchIndexRepository _searchRepo;
        private CustomSearchQueueRepository _queueRepo;

        #endregion // Declarations

        #region Constructors

        public SearchIndexingWorker(string connectionString)
        {
            this._searchRepo = new CustomSearchIndexRepository(connectionString);
            this._queueRepo = new CustomSearchQueueRepository(connectionString);
        }

        #endregion // Constructors

        #region Private Methods

        #endregion // Private Methods

        #region Protected Methods

        #endregion // Protected Methods

        #region Public Methods

        public bool Run()
        {
            bool hasRun = false;

            try
            {
                // Get indexing request from queue
                CustomSearchIndexingRequest request = this._queueRepo.GetQueuedIndexingRequest();

                if (request != null)
                {
                    hasRun = true;

                    try
                    {
                        // Create index writer
                        this._searchRepo.CreateWriter();

                        while (request != null)
                        {
                            if (request.PurgeAll)
                            {
                                this._searchRepo.DeleteAll();
                            }
                            else if (request.Entity != null)
                            {
                                if (request.DeIndex)
                                {
                                    // De-index item
                                    this._searchRepo.DeIndex(request.Entity);
                                }
                                else
                                {
                                    // Index item
                                    this._searchRepo.Index(request.Entity);
                                }
                            }

                            // Get next indexing request from queue
                            request = this._queueRepo.GetQueuedIndexingRequest();

                        } // end while (request != null)

                        // Close index writer, and optimize index
                        this._searchRepo.CloseWriter(true);

                    }
                    catch (Exception ex)
                    {
                        hasRun = false;

                        // Close index writer
                        if (this._searchRepo.Writer != null)
                        {
                            this._searchRepo.ForceClose();
                        }
                    }


                }
            }
            catch (Exception ex)
            {
            }

            return hasRun;
        }

        #endregion // Public Methods

        #region Public Properties

        #endregion // Public Properties

    }
}
