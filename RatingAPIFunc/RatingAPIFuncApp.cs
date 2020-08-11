using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using System.Net.Http;
using Microsoft.Azure.WebJobs.Host;
using System.Collections.Generic;
using System.Linq;

namespace RatingAPIFunc
{
    public static class RatingAPIFuncApp
    {
        // Comments-1
        [FunctionName("CreateRating")]
        public static async Task<IActionResult> CreateRating(
            [HttpTrigger(AuthorizationLevel.Function, "post", Route = null)] HttpRequest req,
            [CosmosDB(
            databaseName: "ratings",
            collectionName: "ratings",
            ConnectionStringSetting = "RatingsDBConnection")]IAsyncCollector<Rating> ratingsOut,
            ILogger log)
        {
            string requestBody = await new StreamReader(req.Body).ReadToEndAsync();
            Rating rating = JsonConvert.DeserializeObject<Rating>(requestBody);

            bool isUserValid = await IsUserIdValidAsync(rating.userId);
            bool isProductValid = await IsProductIdValidAsync(rating.productId);
            if (isProductValid && isUserValid)
            {
                rating.id = Guid.NewGuid().ToString();
                rating.timestamp = DateTime.Now.ToString("u");
                await ratingsOut.AddAsync(rating);
                return new OkObjectResult(JsonConvert.SerializeObject(rating));
            }

            return new BadRequestResult();
        }

        [FunctionName("GetRating")]
        public static async Task<IActionResult> GetRating(
            [HttpTrigger(AuthorizationLevel.Function, "get", Route = null)] HttpRequest req,
            [CosmosDB(
            databaseName: "ratings",
            collectionName: "ratings",
            ConnectionStringSetting = "RatingsDBConnection",
            SqlQuery = "select * from ratings r")]IEnumerable<Rating> ratings,
            ILogger log)
        {
            string ratingId = req.Query["ratingId"];

            if (ratings.Count() > 0 && ratings.FirstOrDefault(r => r.id == ratingId) != null)
            {
                return new OkObjectResult(ratings.First(r => r.id == ratingId));
            }

            return new NotFoundObjectResult("No ratings found for ratingId:" + ratingId);
        }

        [FunctionName("GetRatings")]
        public static async Task<IActionResult> GetRatings(
            [HttpTrigger(AuthorizationLevel.Function, "get", Route = null)] HttpRequest req,
            [CosmosDB(
            databaseName: "ratings",
            collectionName: "ratings",
            ConnectionStringSetting = "RatingsDBConnection",
            SqlQuery = "select * from ratings r")]IEnumerable<Rating> ratings,
            ILogger log)
        {
            string userId = req.Query["userId"];
            bool isUserValid = await IsUserIdValidAsync(userId);

            if (ratings.Count() > 0 && (ratings.FirstOrDefault(r => r.userId == userId) != null) && isUserValid)
            {
                return new OkObjectResult(ratings.Where(r => r.userId == userId).ToList());
            }

            return new NotFoundObjectResult("No ratings found for userId:" + userId);
        }

        private static async Task<bool> IsUserIdValidAsync(string userId)
        {
            HttpClient newClient = new HttpClient();
            HttpRequestMessage newRequest = new HttpRequestMessage(HttpMethod.Get, string.Format("https://serverlessohuser.trafficmanager.net/api/GetUser?userId={0}", userId));
            HttpResponseMessage response = await newClient.SendAsync(newRequest);
            string responseData = await response.Content.ReadAsStringAsync();
            if (responseData.IndexOf(userId) > -1)
            {
                return true;
            }

            return false;
        }

        private static async Task<bool> IsProductIdValidAsync(string productId)
        {
            HttpClient newClient = new HttpClient();
            HttpRequestMessage newRequest = new HttpRequestMessage(HttpMethod.Get, string.Format("https://serverlessohproduct.trafficmanager.net/api/GetProduct?productId={0}", productId));
            HttpResponseMessage response = await newClient.SendAsync(newRequest);
            string responseData = await response.Content.ReadAsStringAsync();
            if (responseData.IndexOf(productId) > -1)
            {
                return true;
            }

            return false;
        }
    }

    public class Rating
    {
        public string id { get; set; }
        public string userId { get; set; }
        public string productId { get; set; }
        public string timestamp { get; set; }
        public string locationName { get; set; }
        public int rating { get; set; }
        public string userNotes { get; set; }
    }
}
