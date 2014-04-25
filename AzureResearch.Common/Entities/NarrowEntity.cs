using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AzureResearch.Common.Entities
{
    public class NarrowEntity : TableEntity
    {
        public DateTime ReportDate { get; set; }
        public string Field01 { get; set; }
        public string Field02 { get; set; }
        public string Field03 { get; set; }
        public string Field04 { get; set; }
        public string Field05 { get; set; }
    }
}
