using System;
using System.Collections.Generic;
using System.Data.Services.Common;
using System.Linq;
using System.Runtime.Serialization;
using System.Text;
using System.Threading.Tasks;

namespace AzureResearch.Common.Entities
{
    [DataServiceEntity]
    [DataContract]
    [DataServiceKey("PartitionKey", "RowKey")]
    public class CustomTableEntity
    {
        public string PartitionKey { get; set; }
        public string RowKey { get; set; }
        public DateTime Timestamp { get; set; }

    }
}
