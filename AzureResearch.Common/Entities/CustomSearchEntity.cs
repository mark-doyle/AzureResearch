using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Microsoft.WindowsAzure.Storage.Table;

namespace AzureResearch.Common.Entities
{
    public class CustomSearchEntity : TableEntity
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

        public string FirstName { get; set; }
        public string LastName { get; set; }
        public string EmailAddress { get; set; }
        public string Gender { get; set; }
        public DateTime DateOfBirth { get; set; }
        public int YearsAtAddress { get; set; }
        public int HeightInInches { get; set; }
        public bool IsMarried { get; set; }

        #endregion // Public Properties

        #region Static Methods / Constants

        public static CustomSearchEntity GetRandom()
        {
            Random random = new Random();
            CustomSearchEntity entity = new CustomSearchEntity()
            {
                PartitionKey = PARTITION_KEYS[random.Next(0, PARTITION_KEYS.Length - 1)],
                RowKey = Guid.NewGuid().ToString(),
                FirstName = FIRST_NAMES[random.Next(0, FIRST_NAMES.Length - 1)],
                LastName = LAST_NAMES[random.Next(0, LAST_NAMES.Length - 1)],
                DateOfBirth = new DateTime(random.Next(1940, 1980), random.Next(1, 12), random.Next(1, 28)),
                YearsAtAddress = random.Next(0, 30),
                HeightInInches = random.Next(60, 75),
                Gender = (random.Next(0, 10) % 2 == 0 ? "M" : "F"),
                IsMarried = (random.Next(0, 10) % 2 == 0),
            };

            entity.EmailAddress = entity.FirstName + "." + entity.LastName + "@" + DOMAINS[random.Next(0, DOMAINS.Length - 1)];

            return entity;
        }

        static string[] PARTITION_KEYS = new string[]  {  "A", "B", "C", "D", "E" };
        static string[] DOMAINS = new string[] 
                                 { "gmail.com", "yahoo.com", "live.com", "hotmail.com", "facebook.com", "me.com", 
                                   "uta.edu", "tcu.edu", "udallas.edu", "twu.edu", "tcc.edu", "unt.edu", "utd.edu",
                                   "lm.com", "bell.com", "aa.com", "abc.com", "aol.com", "myspace.com", "hhs.gov"
                                 };
        static string[] LAST_NAMES = new string[] 
                                 { "Smith", "Johnson", "Williams", "Jones", "Brown", "Davis", "Miller", "Wilson", "Moore", 
                                   "Taylor", "Anderson", "Thomas", "Jackson", "White", "Harris", "Martin", "Thompson", 
                                   "Garcia", "Martinez", "Robinson", "Kirk", "Walker", "Ashton", "Simmons", "Greene"
                                 };
        static string[] FIRST_NAMES = new string[]
                                 {
                                     "Bubba", "James", "Mary", "John", "Patricia", "Robert", "Linda", "Michael", "Veronica",
                                     "Barbara", "William", "Elizabeth", "David", "Jennifer", "Richard", "Maria", "Charles",
                                     "Susan", "Joseph", "Margaret", "Thomas", "Dorothy", "Jeffrey", "Michelle", "Vicky",
                                     "Chester", "George", "Diane", "Lisa", "Katherine"
                                 };

        #endregion // Static Methods / Constants

    }
}
