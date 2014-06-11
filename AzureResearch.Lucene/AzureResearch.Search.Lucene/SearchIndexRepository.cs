using System;
using System.Collections.Generic;
using System.Configuration;
using System.IO;
using System.Linq;
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

namespace AzureResearch.Search.Lucene
{
    /// <summary>
    /// Lucene.NET search provider using Azure
    /// </summary>
    /// <remarks>
    /// Based on information from the following sites:
    /// => http://www.codeproject.com/Articles/320219/Lucene-Net-ultra-fast-search-for-MVC-or-WebForms
    /// => http://codingfields.com/guides/search-on-azure-using-lucene-net/
    /// 
    public abstract class SearchIndexRepository
    {
        #region Declarations

        private string _searchCatalog;
        private AzureDirectory _searchDirectory;
        private RAMDirectory _cacheDirectory;
        private CloudStorageAccount _storageAccount;
        private StandardAnalyzer _analyzer;
        private IndexWriter _indexWriter;

        #endregion // Declarations

        #region Constructors

        /// <summary>
        /// Creates a new search provider
        /// </summary>
        /// <param name="searchCatalog">Catalog name for search index</param>
        /// <param name="cacheInMemory">Indicates whether cache should be file-based or memory-based. Larger indexes probably shouldn't be cached in memory</param>
        /// <param name="connectionString">Azure storage connection string. Uses development storage account if NULL or empty</param>
        public SearchIndexRepository(string searchCatalog, bool cacheInMemory, string connectionString)
        {
            this._storageAccount = (string.IsNullOrEmpty(connectionString) ? CloudStorageAccount.DevelopmentStorageAccount : CloudStorageAccount.Parse(connectionString));

            this._searchCatalog = searchCatalog;
            if (cacheInMemory)
            {
                this._cacheDirectory = new RAMDirectory();
                this._searchDirectory = new AzureDirectory(this._storageAccount, this._searchCatalog, this._cacheDirectory);
            }
            else
            {
                this._searchDirectory = new AzureDirectory(this._storageAccount, this._searchCatalog);
            }
        }

        #endregion // Constructors

        #region Private Methods

        #endregion // Private Methods

        #region Protected Methods

        #endregion // Protected Methods

        #region Public Methods

        /// <summary>
        /// Gets an instantiated index writer
        /// </summary>
        /// <param name="Analyzer">Standard analyzer to use</param>
        /// <returns></returns>
        public IndexWriter GetIndexWriter(StandardAnalyzer Analyzer)
        {
            bool create = !IndexReader.IndexExists(this._searchDirectory);
            return new IndexWriter(
                    this._searchDirectory,
                    Analyzer,
                    create,
                    IndexWriter.MaxFieldLength.UNLIMITED);
        }

        /// <summary>
        /// Gets an instantiated index searcher
        /// </summary>
        /// <param name="ReadOnly"></param>
        /// <returns></returns>
        public IndexSearcher GetIndexSearcher(bool ReadOnly)
        {
            return new IndexSearcher(this._searchDirectory, ReadOnly);
        }

        /// <summary>
        /// Creates an internal index writer and standard analyzer, that will be used for the Add and Delete operations
        /// </summary>
        public void CreateWriter()
        {
            try
            {
                if (this._indexWriter == null)
                {
                    this._analyzer = new StandardAnalyzer(Version.LUCENE_30);
                    this._indexWriter = this.GetIndexWriter(this._analyzer);
                }
            }
            catch (Exception)
            {
                this._searchDirectory.ClearLock("write.lock");
                this._indexWriter = null;
                this._analyzer = null;
            }
        }

        /// <summary>
        /// Closes the internal index writer and standard analyzer, and can optimize before closing
        /// </summary>
        /// <param name="Optimize">Indicates whether index should be optimized</param>
        public void CloseWriter(bool Optimize)
        {
            try
            {
                if (this._indexWriter != null)
                {
                    this._analyzer.Close();

                    if (Optimize)
                    {
                        this._indexWriter.Optimize();
                    }
                    this._indexWriter.Dispose();

                }
            }
            catch (Exception)
            {
                this._searchDirectory.ClearLock("write.lock");
            }
            finally
            {
                this._indexWriter = null;
                this._analyzer = null;
            }
        }

        /// <summary>
        /// Forces the index writer to be closed, in the case of an exception
        /// </summary>
        public void ForceClose()
        {
            try
            {
                if (this._indexWriter != null)
                {
                    this._analyzer.Close();

                    this._indexWriter.Dispose();

                    this._searchDirectory.ClearLock("write.lock");
                }
            }
            catch (Exception)
            {
            }
            finally
            {
                this._indexWriter = null;
                this._analyzer = null;
            }
        }

        /// <summary>
        /// Adds a new document with the specified identifier and fields
        /// </summary>
        /// <param name="Identifier">Unique search document identifier</param>
        /// <param name="Fields">List of Lucene fields to add to document</param>
        /// <remarks>CreateWriter MUST be called prior to using this method. CloseWriter SHOULD be called after documents are added</remarks>
        public void Add(string Identifier, List<Field> Fields)
        {
            if (this._indexWriter == null)
                //throw new ArgumentException("IndexWriter cannot be null. Call CreateWriter() prior to adding or deleting documents from index");
                this.CreateWriter();

            // delete record, if exists
            this.Delete(Identifier);

            // add new index entry
            var doc = new Document();

            // Add document identifier
            doc.Add(new Field("Id", Identifier, Field.Store.YES, Field.Index.NOT_ANALYZED));

            // add lucene fields mapped to db fields
            Fields.ForEach(fld => doc.Add(fld));

            //doc.Add(new Field("Id", sampleData.Id.ToString(), Field.Store.YES, Field.Index.NOT_ANALYZED));
            //doc.Add(new Field("Name", sampleData.Name, Field.Store.YES, Field.Index.ANALYZED));
            //doc.Add(new Field("Description", sampleData.Description, Field.Store.YES, Field.Index.ANALYZED));

            // add entry to index
            this._indexWriter.AddDocument(doc);
        }

        /// <summary>
        /// Deletes a document by the specified identifier
        /// </summary>
        /// <param name="Identifier">Unique search document identifier</param>
        /// <remarks>CreateWriter MUST be called prior to using this method. CloseWriter SHOULD be called after documents are deleted</remarks>
        public void Delete(string Identifier)
        {
            if (this._indexWriter == null)
                //throw new ArgumentException("IndexWriter cannot be null. Call CreateWriter() prior to adding or deleting documents from index");
                this.CreateWriter();

            // remove older index entry
            var searchQuery = new TermQuery(new Term("Id", Identifier));
            this._indexWriter.DeleteDocuments(searchQuery);
        }

        /// <summary>
        /// Deletes all documents within the search index. This method manages its own IndexWriter
        /// </summary>
        public void DeleteAll()
        {
            try
            {
                var analyzer = new StandardAnalyzer(Version.LUCENE_30);
                using (var writer = this.GetIndexWriter(analyzer))
                {
                    // remove older index entries
                    writer.DeleteAll();
                    writer.Commit();

                    // close handles
                    analyzer.Close();
                    writer.Dispose();
                }

            }
            catch (Exception)
            {
                this._searchDirectory.ClearLock("write.lock");
            }
        }

        /// <summary>
        /// Optimizes the search index. This method manages its own IndexWriter
        /// </summary>
        public void Optimize()
        {
            var analyzer = new StandardAnalyzer(Version.LUCENE_30);
            using (var writer = this.GetIndexWriter(analyzer))
            {
                analyzer.Close();
                writer.Optimize();
                writer.Dispose();
            }
        }

        /// <summary>
        /// Parses a query string
        /// </summary>
        /// <param name="SearchQuery">User search terms</param>
        /// <param name="Parser">Instantiated query parser</param>
        /// <returns></returns>
        public Query ParseQuery(string SearchQuery, QueryParser Parser)
        {
            Query query = null;

            try
            {
                query = Parser.Parse(SearchQuery);
            }
            catch (ParseException)
            {
                query = Parser.Parse(QueryParser.Escape(SearchQuery));
            }

            return query;
        }

        /// <summary>
        /// Prepares a user search query for partial word searches
        /// </summary>
        /// <param name="SearchQuery">User search terms</param>
        /// <returns></returns>
        public string PrepareQuery(string SearchQuery)
        {
            if (!string.IsNullOrEmpty(SearchQuery))
            {
                return string.Join(" ", SearchQuery
                    .Trim()
                    .Replace("*", "")
                    .Replace("?", "")
                    .Replace("-", " ")
                    .Replace("#", " ")
                    .Split(' ')
                    .Where(q => !string.IsNullOrEmpty(q))
                    .Select(q => q.Trim() + "*")
                    );
            }
            else
            {
                return SearchQuery;
            }
        }

        #endregion // Public Methods

        #region Public Properties

        /// <summary>
        /// Gets the internal index writer created in CreateWriter
        /// </summary>
        public IndexWriter Writer { get { return this._indexWriter; } }

        /// <summary>
        /// Gets the internal standard analyzer created in CreateWriter
        /// </summary>
        public StandardAnalyzer Analyzer { get { return this._analyzer; } }

        #endregion // Public Properties

    }
}
