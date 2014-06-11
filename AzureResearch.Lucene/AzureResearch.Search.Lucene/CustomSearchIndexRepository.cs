using System;
using System.Collections.Generic;
using System.Configuration;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Web;

using Lucene.Net.Analysis.Standard;
using Lucene.Net.Documents;
using Lucene.Net.Index;
using Lucene.Net.QueryParsers;
using Lucene.Net.Search;
using Lucene.Net.Store;
using Lucene.Net.Store.Azure;
using Version = Lucene.Net.Util.Version;

using Microsoft.WindowsAzure.Storage;

using AzureResearch.Common.Entities;

namespace AzureResearch.Search.Lucene
{
    public class CustomSearchIndexRepository : SearchIndexRepository
    {
        #region Declarations

        #endregion // Declarations

        #region Constants

        public const string FIELD_PARTITION_KEY = "PartitionKey";
        public const string FIELD_ROW_KEY = "RowKey";
        public const string FIELD_FULL_NAME = "FullName";
        public const string FIELD_FIRST_NAME = "FirstName";
        public const string FIELD_LAST_NAME = "LastName";
        public const string FIELD_EMAIL_ADDRESS = "EmailAddress";
        public const string FIELD_GENDER = "Gender";
        public const string FIELD_DATE_OF_BIRTH = "DateOfBirth";
        public const string FIELD_YEARS_AT_ADDRESS = "YearsAtAddress";
        public const string FIELD_HEIGHT_IN_INCHES = "HeightInInches";
        public const string FIELD_IS_MARRIED = "IsMarried";

        #endregion // Constants

        #region Constructors

        public CustomSearchIndexRepository(string connectionString)
            : base("customsearchentity", false, connectionString)
        {
        }

        #endregion // Constructors

        #region Private Methods

        private DateTime _ParseDate(string StoredDate)
        {
            DateTime date = DateTime.MinValue;

            if (!string.IsNullOrEmpty(StoredDate) && StoredDate.Length == 8)
            {
                date = new DateTime(
                    Convert.ToInt32(StoredDate.Substring(0, 4)),
                    Convert.ToInt32(StoredDate.Substring(4, 2)),
                    Convert.ToInt32(StoredDate.Substring(6, 2))
                    );
            }
            else
            {
                DateTime.TryParse(StoredDate, out date);
            }

            return date;
        }

        private CustomSearchEntity _GetResultItemFromDocument(Document Document)
        {
            return new CustomSearchEntity()
            {
                DateOfBirth = this._ParseDate(Document.Get(FIELD_DATE_OF_BIRTH)),
                EmailAddress = Document.Get(FIELD_EMAIL_ADDRESS),
                FirstName = Document.Get(FIELD_FIRST_NAME),
                Gender = Document.Get(FIELD_GENDER),
                HeightInInches = int.Parse(Document.Get(FIELD_HEIGHT_IN_INCHES)),
                IsMarried = bool.Parse(Document.Get(FIELD_IS_MARRIED)),
                LastName = Document.Get(FIELD_LAST_NAME),
                PartitionKey = Document.Get(FIELD_PARTITION_KEY),
                RowKey = Document.Get(FIELD_ROW_KEY),
                YearsAtAddress = int.Parse(Document.Get(FIELD_YEARS_AT_ADDRESS)),
            };
        }

        private List<CustomSearchEntity> _MapDocumentsToResults(IEnumerable<Document> Hits)
        {
            return Hits.Select(_GetResultItemFromDocument).ToList();
        }

        private List<CustomSearchEntity> _MapDocumentsToResults(IEnumerable<ScoreDoc> Hits, IndexSearcher Searcher)
        {
            return Hits.Select(hit => _GetResultItemFromDocument(Searcher.Doc(hit.Doc))).ToList();
        }

        #endregion // Private Methods

        #region Protected Methods

        #endregion // Protected Methods

        #region Public Methods

        public void DeIndex(CustomSearchEntity entity)
        {
            // Document identifier
            string identifier = entity.PartitionKey + "-" + entity.RowKey;

            this.Delete(identifier);

        }

        public void Index(CustomSearchEntity entity)
        {
            // Document identifier
            string identifier = entity.PartitionKey + "-" + entity.RowKey;

            string fullName = entity.FirstName + " " + entity.LastName + " " + entity.EmailAddress;

            // Document fields
            List<Field> fields = new List<Field>()
            {
                new Field(FIELD_PARTITION_KEY, entity.PartitionKey.ToString(), Field.Store.YES, Field.Index.NOT_ANALYZED),
                new Field(FIELD_ROW_KEY, entity.RowKey.ToString(), Field.Store.YES, Field.Index.NOT_ANALYZED),
                new Field(FIELD_EMAIL_ADDRESS, entity.EmailAddress, Field.Store.YES, Field.Index.NOT_ANALYZED),
                new Field(FIELD_FULL_NAME, fullName, Field.Store.NO, Field.Index.ANALYZED, Field.TermVector.WITH_POSITIONS),
                new Field(FIELD_FIRST_NAME, entity.FirstName, Field.Store.YES, Field.Index.NOT_ANALYZED),
                new Field(FIELD_LAST_NAME, entity.LastName, Field.Store.YES, Field.Index.NOT_ANALYZED),
                new Field(FIELD_GENDER, entity.Gender, Field.Store.YES, Field.Index.NOT_ANALYZED),
                new Field(FIELD_DATE_OF_BIRTH, entity.DateOfBirth.ToString("yyyyMMdd"), Field.Store.YES, Field.Index.NOT_ANALYZED),
                new Field(FIELD_IS_MARRIED, entity.IsMarried.ToString(), Field.Store.YES, Field.Index.NOT_ANALYZED),
                new Field(FIELD_YEARS_AT_ADDRESS, entity.YearsAtAddress.ToString(), Field.Store.YES, Field.Index.NOT_ANALYZED),
                new Field(FIELD_HEIGHT_IN_INCHES, entity.HeightInInches.ToString(), Field.Store.YES, Field.Index.NOT_ANALYZED)
            };

            // Add search document
            this.Add(identifier, fields);

            // Clean up
            fields = null;
            identifier = null;
        }

        public SearchResult<CustomSearchEntity> SearchByFields(string firstName, string lastName, string emailAddress, string gender, DateTime? dateOfBirth, int? yearsAtAddress, int? heightInInches, bool? isMarried)
        {
            SearchResult<CustomSearchEntity> searchResult = new SearchResult<CustomSearchEntity>();

            using (IndexSearcher searcher = this.GetIndexSearcher(true))
            {
                StandardAnalyzer analyzer = new StandardAnalyzer(Version.LUCENE_30);

                BooleanQuery complexQuery = new BooleanQuery();

                if (!string.IsNullOrEmpty(firstName))
                    complexQuery.Add(new TermQuery(new Term(FIELD_FIRST_NAME, firstName)), Occur.MUST);
                if (!string.IsNullOrEmpty(lastName))
                    complexQuery.Add(new TermQuery(new Term(FIELD_LAST_NAME, lastName)), Occur.MUST);
                if (!string.IsNullOrEmpty(emailAddress))
                    complexQuery.Add(new TermQuery(new Term(FIELD_EMAIL_ADDRESS, emailAddress)), Occur.MUST);
                if (!string.IsNullOrEmpty(gender))
                    complexQuery.Add(new TermQuery(new Term(FIELD_GENDER, gender)), Occur.MUST);

                if (dateOfBirth.HasValue)
                    complexQuery.Add(new TermQuery(new Term(FIELD_DATE_OF_BIRTH, dateOfBirth.Value.ToString("yyyyMMdd"))), Occur.MUST);
                if (yearsAtAddress.HasValue)
                    complexQuery.Add(new TermQuery(new Term(FIELD_YEARS_AT_ADDRESS, yearsAtAddress.Value.ToString())), Occur.MUST);
                if (heightInInches.HasValue)
                    complexQuery.Add(new TermQuery(new Term(FIELD_HEIGHT_IN_INCHES, heightInInches.Value.ToString())), Occur.MUST);
                if (isMarried.HasValue)
                    complexQuery.Add(new TermQuery(new Term(FIELD_IS_MARRIED, isMarried.Value.ToString())), Occur.MUST);

                var results = searcher.Search(complexQuery, null, 100, Sort.RELEVANCE);
                searchResult.ResultCount = results.TotalHits;
                searchResult.Results = this._MapDocumentsToResults(results.ScoreDocs, searcher);

                analyzer.Close();
                searcher.Dispose();
            }

            return searchResult;
        }

        public SearchResult<CustomSearchEntity> SearchByPartialNameAndGender(string partialName, string gender)
        {
            SearchResult<CustomSearchEntity> searchResult = new SearchResult<CustomSearchEntity>();

            string modifiedKeywordQuery = this.PrepareQuery(partialName);

            using (IndexSearcher searcher = this.GetIndexSearcher(true))
            {
                StandardAnalyzer analyzer = new StandardAnalyzer(Version.LUCENE_30);
                QueryParser nameParser = null;

                BooleanQuery complexQuery = new BooleanQuery();

                complexQuery.Add(new TermQuery(new Term(FIELD_GENDER, gender)), Occur.MUST);

                // Add keywords to search
                nameParser = new QueryParser(Version.LUCENE_30, FIELD_FULL_NAME, analyzer);
                complexQuery.Add(this.ParseQuery(modifiedKeywordQuery, nameParser), Occur.MUST);
                //complexQuery.Add(new WildcardQuery(new Term(FIELD_BODY, modifiedKeywordQuery)), Occur.MUST);


                var results = searcher.Search(complexQuery, null, 100, Sort.RELEVANCE);
                searchResult.ResultCount = results.TotalHits;
                searchResult.Results = this._MapDocumentsToResults(results.ScoreDocs, searcher);

                analyzer.Close();
                searcher.Dispose();
            }

            return searchResult;
        }

        public SearchResult<CustomSearchEntity> SearchByHeightRange(int minHeight, int maxHeight)
        {
            SearchResult<CustomSearchEntity> searchResult = new SearchResult<CustomSearchEntity>();

            using (IndexSearcher searcher = this.GetIndexSearcher(true))
            {
                StandardAnalyzer analyzer = new StandardAnalyzer(Version.LUCENE_30);

                BooleanQuery complexQuery = new BooleanQuery();

                // Add height range to search
                if (minHeight > 0 && maxHeight > 0 && minHeight <= maxHeight)
                {
                    BooleanQuery heightQuery = new BooleanQuery();
                    int current = minHeight;
                    while (current <= maxHeight)
                    {
                        heightQuery.Add(new TermQuery(new Term(FIELD_HEIGHT_IN_INCHES, current.ToString())), Occur.SHOULD);

                        current++;
                    }
                    complexQuery.Add(heightQuery, Occur.MUST);

                }

                var results = searcher.Search(complexQuery, null, 100, Sort.RELEVANCE);
                searchResult.ResultCount = results.TotalHits;
                searchResult.Results = this._MapDocumentsToResults(results.ScoreDocs, searcher);

                analyzer.Close();
                searcher.Dispose();
            }

            return searchResult;
        }

        #endregion // Public Methods

        #region Public Properties

        #endregion // Public Properties

    }
}
