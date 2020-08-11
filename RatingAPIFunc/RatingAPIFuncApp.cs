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
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Microsoft.Extensions.Configuration;
using Newtonsoft.Json.Linq;
using Microsoft.Azure.Documents;
using Microsoft.Azure.EventHubs;
using System.Text;

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

        [FunctionName("ProcessBtachFiles")]
        public static async Task<IActionResult> ProcessBatchFiles(
            [HttpTrigger(AuthorizationLevel.Function, "post", Route = null)] HttpRequest req,
            [CosmosDB(
            databaseName: "ratings",
            collectionName: "orders",
            ConnectionStringSetting = "RatingsDBConnection")]IAsyncCollector<Document> orders,
            ILogger log, ExecutionContext context)
        {
            var config = new ConfigurationBuilder()
                .SetBasePath(context.FunctionAppDirectory)
                .AddJsonFile("local.settings.json", optional: true, reloadOnChange: true)
                .AddEnvironmentVariables()
                .Build();
            // Create a BlobServiceClient object which will be used to create a container client
            BlobServiceClient blobServiceClient = new BlobServiceClient(config["BatchStorageAccount"]);

            // Create the container and return a container client object
            BlobContainerClient containerClient = blobServiceClient.GetBlobContainerClient("datafiles");

            List<string> uniquePrefixes = new List<string>();
            foreach (BlobItem blobItem in containerClient.GetBlobs())
            {
                string prefix = blobItem.Name.Substring(0, blobItem.Name.IndexOf("-"));
                if (!uniquePrefixes.Contains(prefix))
                {
                    uniquePrefixes.Add(prefix);
                }                
            }

            foreach (string prefix in uniquePrefixes)
            {
                var blobs = containerClient.GetBlobs(BlobTraits.None, BlobStates.None, prefix);
                if (blobs.Count() == 3)
                {
                    string result = await CobmineFiles(prefix);
                    JArray orderArray = JArray.Parse(result);
                    foreach(JObject order in orderArray)
                    {
                        string json = JsonConvert.SerializeObject(order);
                        using (JsonTextReader reader = new JsonTextReader(new StringReader(json)))
                        {
                            var document = new Document();
                            document.LoadFrom(reader);
                            await orders.AddAsync(document);
                        }
                    }

                    // Delete blobs
                    foreach(var blob in blobs)
                    {
                        containerClient.DeleteBlob(blob.Name);
                    }

                    Console.WriteLine("Processed prefix:" + prefix);
                }
            }

            return new OkObjectResult(uniquePrefixes.Count());
        }

        [FunctionName("ProcessPOSEvents")]
        public static async Task ProcessPOSEvents(
            [EventHubTrigger("samples-workitems", Connection = "EventHubConnectionAppSetting")] EventData[] eventHubMessages,
            [CosmosDB(
            databaseName: "ratings",
            collectionName: "posEvents",
            ConnectionStringSetting = "RatingsDBConnection")]IAsyncCollector<Document> posEvents, 
            ILogger log)
        {
            foreach (var message in eventHubMessages)
            {
                try
                {
                    string json = JsonConvert.SerializeObject(Encoding.UTF8.GetString(message.Body));
                    json = json.Replace("\\", "");
                    json = json.Substring(1, json.Length - 2);
                    JObject jObject = JObject.Parse(json);
                    json = JsonConvert.SerializeObject(jObject);
                    using (JsonTextReader reader = new JsonTextReader(new StringReader(json)))
                    {
                        var document = new Document();
                        document.LoadFrom(reader);
                        await posEvents.AddAsync(document);
                    }
                    log.LogInformation($"C# function triggered to process a message: {Encoding.UTF8.GetString(message.Body)}");
                    log.LogInformation($"EnqueuedTimeUtc={message.SystemProperties.EnqueuedTimeUtc}");
                }
                catch (Exception ex)
                {
                    log.LogInformation($"C# function triggered to process a message : {Encoding.UTF8.GetString(message.Body)} with error: {ex.Message}");
                    log.LogInformation($"EnqueuedTimeUtc={message.SystemProperties.EnqueuedTimeUtc}");
                }
            }
        }

        private static async Task<string> CobmineFiles(string prefix)
        {
            string jsonData = @"{
                                  'orderHeaderDetailsCSVUrl': 'https://nzserverlessohbatchdata.blob.core.windows.net/datafiles/XXXXXXXXXXXXXX-OrderHeaderDetails.csv',
                                  'orderLineItemsCSVUrl': 'https://nzserverlessohbatchdata.blob.core.windows.net/datafiles/XXXXXXXXXXXXXX-OrderLineItems.csv',
                                  'productInformationCSVUrl': 'https://nzserverlessohbatchdata.blob.core.windows.net/datafiles/XXXXXXXXXXXXXX-ProductInformation.csv'
                                }";
            jsonData = jsonData.Replace("XXXXXXXXXXXXXX", prefix);
            jsonData = jsonData.Replace("'", "\"");
            var content = new StringContent(jsonData,
                                          System.Text.Encoding.UTF8,
                                          "application/json"
                                          );
            HttpClient newClient = new HttpClient();
            HttpResponseMessage response = await newClient.PostAsync("https://serverlessohmanagementapi.trafficmanager.net/api/order/combineOrderContent", content);
            string responseData = await response.Content.ReadAsStringAsync();

            JArray orders = JArray.Parse(responseData);
            
            return responseData;
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

    public class OrderHeaderDetails
    {
        public string ponumber { get; set; }
        public string datetime { get; set; }
        public string locationid { get; set; }
        public string locationname { get; set; }
        public string locationaddress { get; set; }
        public string locationpostcode { get; set; }
        public string totalcost { get; set; }
        public string totaltax { get; set; }
    }

    public class OrderLineItem
    {
        public string ponumber { get; set; }
        public string productid { get; set; }
        public string quantity { get; set; }
        public string unitcost { get; set; }
        public string totalcost { get; set; }
        public string totaltax { get; set; }
    }

    //public class 
}
