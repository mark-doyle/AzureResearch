using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.ServiceRuntime;

namespace AzureResearch.Search.Lucene
{
    public class SearchIndexingWorkerRole : RoleEntryPoint
    {
        #region Declarations

        private bool _keepRunning;
        private SearchIndexingWorker _worker;
        private int _timeout;

        #endregion // Declarations

        #region Constructors

        #endregion // Constructors

        #region Private Methods

        #endregion // Private Methods

        #region Protected Methods

        #endregion // Protected Methods

        #region Public Methods

        public override bool OnStart()
        {
            int.TryParse(ConfigurationManager.AppSettings["WorkerTimeout"], out _timeout);
            this._worker = new SearchIndexingWorker(ConfigurationManager.AppSettings["AzureConnection"]);
            this._keepRunning = true;
            return true;
        }

        public override void OnStop()
        {
            this._keepRunning = false;
        }

        public override void Run()
        {
            try
            {
                while (_keepRunning)
                {
                    try
                    {
                        while (this._worker.Run()) { }
                    }
                    catch (Exception ex)
                    {
                    }
                    Thread.Sleep(this._timeout);
                }
            }
            catch (Exception ex)
            {
            }
        }

        #endregion // Public Methods

        #region Public Properties

        #endregion // Public Properties

    }
}
