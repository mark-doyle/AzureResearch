using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Queue;


namespace AzureResearch.Common.Repositories
{
    public class QueueStorageRepository
    {
        #region Declarations

        private string _queueName;
        private CloudQueue _queue;

        #endregion // Declarations

        #region Constructors

        public QueueStorageRepository(string queueName)
            : this(CloudStorageAccount.DevelopmentStorageAccount, queueName)
        {
        }

        public QueueStorageRepository(string connectionString, string queueName)
            : this(CloudStorageAccount.Parse(connectionString), queueName)
        {
        }

        public QueueStorageRepository(CloudStorageAccount cloudStorageAccount, string queueName)
        {
            this._queueName = queueName;

            this._queue = cloudStorageAccount.CreateCloudQueueClient().GetQueueReference(queueName);
            this._queue.CreateIfNotExists();
        }

        #endregion // Constructors

        #region Private Methods

        #endregion // Private Methods

        #region Protected Methods

        #endregion // Protected Methods

        #region Public Methods

        /// <summary>
        /// Retrieves the messages in the queue
        /// </summary>
        /// <param name="messageCount">Number of messages that need to be retrieved</param>
        /// <returns></returns>
        public IEnumerable<CloudQueueMessage> RetrieveMessages(int messageCount)
        {
            return this._queue.GetMessages(messageCount);
        }

        /// <summary>
        /// Gets a single message from the queue
        /// </summary>
        /// <returns>CloudQueueMessage</returns>
        public CloudQueueMessage RetrieveMessage()
        {
            return this._queue.GetMessage();
        }

        /// <summary>
        /// Insert a new text message to the queue
        /// </summary>
        /// <param name="entity">The text message which needs to be inserted</param>
        public void InsertMessage(string textMessage)
        {
            CloudQueueMessage message = new CloudQueueMessage(textMessage);
            this._queue.AddMessage(message);
        }

        /// <summary>
        /// Insert a new text message to the queue
        /// </summary>
        /// <param name="textMessage">The text message which needs to be inserted</param>
        /// <param name="messageVisible">DateTime when message should become visible in the queue</param>
        public void InsertMessage(string textMessage, DateTime messageVisible)
        {
            CloudQueueMessage message = new CloudQueueMessage(textMessage);
            TimeSpan? delay = null;
            if (messageVisible > DateTime.UtcNow)
            {
                delay = messageVisible.Subtract(DateTime.UtcNow);
            }
            this._queue.AddMessage(message, null, delay, null, null);
        }

        /// <summary>
        /// Insert a new byte array message to the queue
        /// </summary>
        /// <param name="byteArrayMessage">The byte array message wich needs to be inserted</param>
        public void InsertMessage(byte[] byteArrayMessage)
        {
            CloudQueueMessage message = new CloudQueueMessage(byteArrayMessage);
            this._queue.AddMessage(message);
        }

        /// <summary>
        /// Insert a new byte array message to the queue
        /// </summary>
        /// <param name="byteArrayMessage">The byte array message wich needs to be inserted</param>
        /// <param name="messageVisible">DateTime when message should become visible in the queue</param>
        public void InsertMessage(byte[] byteArrayMessage, DateTime messageVisible)
        {
            CloudQueueMessage message = new CloudQueueMessage(byteArrayMessage);
            TimeSpan? delay = null;
            if (messageVisible > DateTime.UtcNow)
            {
                delay = messageVisible.Subtract(DateTime.UtcNow);
            }
            this._queue.AddMessage(message, null, delay, null, null);
        }

        /// <summary>
        /// Insert a new cloud queue message to the queue
        /// </summary>
        /// <param name="cloudQueueMessage">CloudQueueMessage</param>
        public void InsertMessage(CloudQueueMessage cloudQueueMessage)
        {
            this._queue.AddMessage(cloudQueueMessage);
        }

        /// <summary>
        /// Delete a queue message
        /// </summary>
        /// <param name="message"></param>
        public void DeleteMessage(CloudQueueMessage message)
        {
            this._queue.DeleteMessage(message);
        }

        /// <summary>
        /// Delete a queue message
        /// </summary>
        /// <param name="messageId"></param>
        /// <param name="popReceipt"></param>
        public void DeleteMessage(string messageId, string popReceipt)
        {
            this._queue.DeleteMessage(messageId, popReceipt);
        }

        /// <summary>
        /// Clear all messages of the queue
        /// </summary>
        public void CleanQueue()
        {
            this._queue.Clear();
        }

        /// <summary>
        /// Delete the queue
        /// </summary>
        public void DeleteQueue()
        {
            this._queue.Delete();
        }

        #endregion // Public Methods

        #region Public Properties

        #endregion // Public Properties

    }
}
