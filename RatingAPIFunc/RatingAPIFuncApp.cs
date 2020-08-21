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
using Microsoft.Azure.WebJobs.ServiceBus;
using Microsoft.Azure.ServiceBus;
using System.Net;
using Microsoft.Azure.Storage.Blob;
using Microsoft.ApplicationInsights.Extensibility;
using Microsoft.ApplicationInsights;
using Azure.AI.TextAnalytics;
using Azure;
using Microsoft.Azure.Documents.SystemFunctions;
using Microsoft.ApplicationInsights.DataContracts;

namespace RatingAPIFunc
{
    public class RatingAPIFuncApp
    {
        //private readonly TelemetryClient telemetryClient;

        //public RatingAPIFuncApp(TelemetryConfiguration telemetryConfiguration)
        //{
        //    this.telemetryClient = new TelemetryClient(telemetryConfiguration);
        //}

        // Comments-1
        [FunctionName("CreateRating")]
        public async Task<IActionResult> CreateRating(
            [HttpTrigger(AuthorizationLevel.Function, "post", Route = null)] HttpRequest req,
            [CosmosDB(
            databaseName: "ratings",
            collectionName: "ratings",
            ConnectionStringSetting = "RatingsDBConnection")]IAsyncCollector<Rating> ratingsOut,
            [EventHub("sentimenthub", Connection = "SentimentEventHubConnectionAppSetting")] IAsyncCollector<EventData> outputEvents,
            ExecutionContext context,
            ILogger log)
        {
            string requestBody = await new StreamReader(req.Body).ReadToEndAsync();
            Rating rating = JsonConvert.DeserializeObject<Rating>(requestBody);

            bool isUserValid = await IsUserIdValidAsync(rating.userId);
            string productName = await GetProductNameAsync(rating.productId);
            if (!String.IsNullOrEmpty(productName) && isUserValid)
            {
                rating.sentimentScore = await GetReviewSentimentScore(rating.userNotes, context);
                string ratingJSON = JsonConvert.SerializeObject(rating);

                // Set Metric // Need to comment when deploying to Azure
                //var metric = new MetricTelemetry("NZ Sentiment", rating.sentimentScore);
                //this.telemetryClient.TrackMetric(metric);

                rating.id = Guid.NewGuid().ToString();
                rating.timestamp = DateTime.Now.ToString("u");
                await ratingsOut.AddAsync(rating);

                // Send to Event Hub
                RatingInfo ratingInfo = new RatingInfo
                {
                    IceCreamFlavor = productName,
                    SentimentScore = rating.sentimentScore
                };
                var eventDataPaylod = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(ratingInfo));
                var eventData = new EventData(eventDataPaylod);
                await outputEvents.AddAsync(eventData);

                return new OkObjectResult(ratingJSON);
            }

            return new BadRequestResult();
        }

        [FunctionName("GetRating")]
        public async Task<IActionResult> GetRating(
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
        public async Task<IActionResult> GetRatings(
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

        /*
        [FunctionName("ProcessBtachFiles")]
        public async Task<IActionResult> ProcessBatchFiles(
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
        public async Task ProcessPOSEvents(
            [EventHubTrigger("nzserverlessohhub", Connection = "EventHubConnectionAppSetting")] EventData[] eventHubMessages,
            [CosmosDB(
            databaseName: "ratings",
            collectionName: "posEvents",
            ConnectionStringSetting = "RatingsDBConnection")]IAsyncCollector<Document> posEvents,
            [ServiceBus("receipts", EntityType.Topic, Connection = "ServiceBusConnection")] IAsyncCollector<Message> messages,
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

                    if (jObject["header"]["receiptUrl"].ToString().Length > 0)
                    {
                        await messages.AddAsync(await BuildQueueMessage(jObject, log));
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

        [FunctionName("ProcessHighValueReceipts")]
        public async Task ProcessHighValueReceipts(
                    [ServiceBusTrigger("receipts", "receipts-high-value", Connection = "ServiceBusConnection")]
                    ReceiptInfo receiptInfo,
                    Int32 deliveryCount,
                    DateTime enqueuedTimeUtc,
                    string messageId,
                    ExecutionContext context,
                    [Blob("receipts-high-value", Connection = "ReceiptsStorageAccount")] CloudBlobContainer blobContainer,
                    ILogger log)
        {
            string base64EncodedReceipt = await Base64EncodeReceipt(receiptInfo.receiptUrl, context);
            HighValueReceipt highValueReceipt = new HighValueReceipt
            {
                Items = receiptInfo.totalItems,
                ReceiptImage = base64EncodedReceipt,
                SalesDate = receiptInfo.salesDate,
                SalesNumber = receiptInfo.salesNumber,
                Store = receiptInfo.storeLocation,
                TotalCost = receiptInfo.totalCost
            };

            log.LogInformation($"Base64 Encoded Receipt={base64EncodedReceipt}");

            string blobName = Guid.NewGuid().ToString("D");
            CloudBlockBlob blob = blobContainer.GetBlockBlobReference($"{blobName}");
            // use Upload* method according to your need
            await blob.UploadTextAsync(JsonConvert.SerializeObject(highValueReceipt));
        }

        [FunctionName("ProcessGeneralReceipts")]
        public async Task ProcessGeneralReceipts(
                    [ServiceBusTrigger("receipts", "receipts", Connection = "ServiceBusConnection")]
                    ReceiptInfo receiptInfo,
                    Int32 deliveryCount,
                    DateTime enqueuedTimeUtc,
                    string messageId,
                    ExecutionContext context,
                    [Blob("receipts", Connection = "ReceiptsStorageAccount")] CloudBlobContainer blobContainer,
                    ILogger log)
        {
            GeneralReceipt generalReceipt = new GeneralReceipt
            {
                Items = receiptInfo.totalItems,
                SalesDate = receiptInfo.salesDate,
                SalesNumber = receiptInfo.salesNumber,
                Store = receiptInfo.storeLocation,
                TotalCost = receiptInfo.totalCost
            };

            string blobName = Guid.NewGuid().ToString("D");
            CloudBlockBlob blob = blobContainer.GetBlockBlobReference($"{blobName}");
            // use Upload* method according to your need
            await blob.UploadTextAsync(JsonConvert.SerializeObject(generalReceipt));
        }

        */

        private async Task<double> GetReviewSentimentScore(string reviewText, ExecutionContext context)
        {
            var config = new ConfigurationBuilder()
                .SetBasePath(context.FunctionAppDirectory)
                .AddJsonFile("local.settings.json", optional: true, reloadOnChange: true)
                .AddEnvironmentVariables()
                .Build();
            // Create a BlobServiceClient object which will be used to create a container client
            var client = new TextAnalyticsClient(new Uri(config["AIEndpoint"]), new AzureKeyCredential(config["AICredentials"]));
            DocumentSentiment documentSentiment = client.AnalyzeSentiment(reviewText);
            return documentSentiment.ConfidenceScores.Positive;
        }

        private async Task<Message> BuildQueueMessage(JObject posJson, ILogger log)
        {
            ReceiptInfo receiptInfo = new ReceiptInfo();
            receiptInfo.receiptUrl = posJson["header"]["receiptUrl"].ToString();
            receiptInfo.storeLocation = posJson["header"]["locationId"].ToString();
            receiptInfo.salesDate = posJson["header"]["dateTime"].ToString();
            receiptInfo.salesNumber = posJson["header"]["salesNumber"].ToString();
            receiptInfo.totalCost = double.Parse(posJson["header"]["totalCost"].ToString());

            JArray quantityArray = (JArray)posJson["details"];            
            foreach (JObject qObj in quantityArray)
            {
                receiptInfo.totalItems = receiptInfo.totalItems + int.Parse(qObj["quantity"].ToString());
            }

            // Create Message ans set its properties
            var message = new Message(Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(receiptInfo)));
            message.UserProperties.Add("totalCost", receiptInfo.totalCost);

            log.LogInformation($"C# function sent a message to topic: {JsonConvert.SerializeObject(receiptInfo)} ");
            return message;
        }

        private async Task<string> CobmineFiles(string prefix)
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

        private async Task<bool> IsUserIdValidAsync(string userId)
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

        private async Task<bool> IsProductIdValidAsync(string productId)
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

        private async Task<string> GetProductNameAsync(string productId)
        {
            HttpClient newClient = new HttpClient();
            HttpRequestMessage newRequest = new HttpRequestMessage(HttpMethod.Get, string.Format("https://serverlessohproduct.trafficmanager.net/api/GetProduct?productId={0}", productId));
            HttpResponseMessage response = await newClient.SendAsync(newRequest);
            string responseData = await response.Content.ReadAsStringAsync();
            if (responseData.IndexOf(productId) > -1)
            {
                JObject productObject = JObject.Parse(responseData);
                return productObject["productName"].ToString();
            }

            return "";
        }

        private async Task<string> Base64EncodeReceipt(string fileLocation, ExecutionContext context)
        {
            WebClient webClient = new WebClient();
            string downloadedFilePath = Path.Combine(context.FunctionDirectory, Path.GetTempFileName());
            webClient.DownloadFile(fileLocation, downloadedFilePath);
            byte[] fileBytes = await File.ReadAllBytesAsync(downloadedFilePath);
            return Convert.ToBase64String(fileBytes);
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
        public double sentimentScore { get; set; }
    }

    public class RatingInfo
    {
        public string IceCreamFlavor { get; set; }
        public double SentimentScore { get; set; }
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

    public class ReceiptInfo
    {
        public int totalItems { get; set; }
        public double totalCost { get; set; }
        public string salesNumber { get; set; }
        public string salesDate { get; set; }
        public string storeLocation { get; set; }
        public string receiptUrl { get; set; }
    }

    public class HighValueReceipt
    {
        public string Store { get; set; }
        public string SalesNumber { get; set; }
        public double TotalCost { get; set; }
        public int Items { get; set; }
        public string SalesDate { get; set; }
        public string ReceiptImage { get; set; }
    }


    public class GeneralReceipt
    {
        public string Store { get; set; }
        public string SalesNumber { get; set; }
        public double TotalCost { get; set; }
        public int Items { get; set; }
        public string SalesDate { get; set; }
    }
}
