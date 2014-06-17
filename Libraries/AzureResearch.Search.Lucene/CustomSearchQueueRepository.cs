using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml;
using System.Xml.Serialization;
using AzureResearch.Common.Constants;
using AzureResearch.Common.Entities;
using AzureResearch.Common.Repositories;

namespace AzureResearch.Search.Lucene
{
    public class CustomSearchQueueRepository : QueueStorageRepository
    {
        #region Declarations

        private XmlSerializer _serializer;

        #endregion // Declarations

        #region Constructors

        public CustomSearchQueueRepository(string connectionString)
            : base(connectionString, QueueNames.CustomSearchQueue)
        {
            this._serializer = new XmlSerializer(typeof(CustomSearchIndexingRequest));
        }

        #endregion // Constructors

        #region Private Methods

        #endregion // Private Methods

        #region Protected Methods

        #endregion // Protected Methods

        #region Public Methods

        public void QueueIndexingRequest(CustomSearchIndexingRequest request)
        {
            try
            {
                string message = null;
                using (StringWriter stringWriter = new StringWriter())
                {
                    using (XmlWriter xmlWriter = XmlWriter.Create(stringWriter))
                    {
                        this._serializer.Serialize(xmlWriter, request);
                    }
                    message = stringWriter.ToString();
                }
                base.InsertMessage(message);
            }
            catch (Exception)
            {
            }
        }

        public CustomSearchIndexingRequest GetQueuedIndexingRequest()
        {
            CustomSearchIndexingRequest request = null;

            try
            {
                var message = base.RetrieveMessage();
                if (message != null)
                {
                    request = (CustomSearchIndexingRequest)this._serializer.Deserialize(new StringReader(message.AsString));
                    //using (MemoryStream stream = new MemoryStream())
                    //{
                    //    using (StreamWriter writer = new StreamWriter(stream))
                    //    {
                    //        writer.Write(message.AsString);
                    //        writer.Flush();
                    //        request = this._serializer.Deserialize(stream) as CustomSearchIndexingRequest;
                    //    }
                    //    //request = this._serializer.Deserialize(stream) as CustomSearchIndexingRequest;
                    //}

                    base.DeleteMessage(message);
                }
            }
            catch (Exception)
            {
                //this.HandleException(ex, "GetQueuedIndexingRequest");
            }

            return request;
        }

        #endregion // Public Methods

        #region Public Properties

        #endregion // Public Properties

    }
}
