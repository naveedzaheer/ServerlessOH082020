using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;

namespace StarFruit
{
    public static class StartFruitFunc
    {
        [FunctionName("StartFruitFuncGet")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = null)] HttpRequest req,
            ILogger log)
        {
            log.LogInformation("C# HTTP trigger function processed a request.");

            string productId = req.Query["productId"];

            string responseMessage = string.IsNullOrEmpty(productId)
                ? "This HTTP triggered function executed successfully. Pass a productId in the query string or in the request body for a personalized response."
                : $"The product name for your product id {productId} is Starfruit Explosion";

            return new OkObjectResult(responseMessage);
        }
    }
}
