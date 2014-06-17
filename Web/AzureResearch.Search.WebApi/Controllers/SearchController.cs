using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using System.Web.Http;
using AzureResearch.Common.Entities;
using AzureResearch.Search.Lucene;

namespace AzureResearch.Search.WebApi.Controllers
{
    [RoutePrefix("Api/Search")]
    public class SearchController : ApiController
    {
        private static CustomSearchIndexRepository _searchRepository;
        private static string _connectionString;

        public static CustomSearchIndexRepository SearchRepository
        {
            get
            {
                if (_searchRepository == null)
                {
                    _searchRepository = new CustomSearchIndexRepository(ConnectionString, true);
                }
                return _searchRepository;
            }
        }

        public static string ConnectionString
        {
            get
            {
                if (string.IsNullOrEmpty(_connectionString))
                {
                    _connectionString = ConfigurationManager.AppSettings["AzureConnection"];
                }
                return _connectionString;
            }
        }

        [HttpGet]
        public SearchResult<CustomSearchEntity> SearchByFields(string firstName, string lastName, string emailAddress, string gender, DateTime? dateOfBirth, int? yearsAtAddress, int? heightInInches, bool? isMarried)
        {
            return SearchRepository.SearchByFields(firstName, lastName, emailAddress, gender, dateOfBirth, yearsAtAddress, heightInInches, isMarried);
        }

        [HttpGet]
        public SearchResult<CustomSearchEntity> SearchByPartialNameAndGender(string partialName, string gender)
        {
            return SearchRepository.SearchByPartialNameAndGender(partialName, gender);
        }

        [HttpGet]
        public SearchResult<CustomSearchEntity> SearchByHeightRange(int minHeight, int maxHeight)
        {
            return SearchRepository.SearchByHeightRange(minHeight, maxHeight);
        }

        [HttpGet]
        public HttpResponseMessage PurgeAllData()
        {
            Parallel.Invoke(
            () =>
                {
                    CustomSearchTableRepository tableRepo = new CustomSearchTableRepository(ConnectionString);
                    var entities = tableRepo.GetAllEntities();
                    tableRepo.DeleteBatch(entities);
                },
            () =>
                {
                    SearchRepository.DeleteAll();
                }
            );

            return new HttpResponseMessage(HttpStatusCode.OK);

        }

    }
}
