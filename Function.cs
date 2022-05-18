using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Amazon.Athena;
using Amazon.Athena.Model;
using Amazon.Lambda.Core;

// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.SystemTextJson.DefaultLambdaJsonSerializer))]

namespace Genrate_Batches
{
    public class Function
    {
        IAmazonAthena AthenaClient { get; set; }

        public Function()
        {
            AthenaClient = new AmazonAthenaClient();
        }

        public Function(IAmazonAthena amazonAthena)
        {
            this.AthenaClient = amazonAthena;
        }
        //public static object get_var_char_values(object d)
        //{
        //    return (from obj in d["Data"] select obj["VarCharValue"]).ToList();
        //}

        public static object generate_batches(int rowcount)
        {
            var batches = new List<object>();
            var batchSize = 10000;
            var batchNumber = 0;
            while (rowcount > 0)
            {
                batches.Append(new Dictionary<object, object> {
                    {
                        "offset",
                        batchSize * batchNumber},
                    {
                        "limit",
                        batchSize},
                    {
                        "batchNumber",
                        batchNumber + 1}});
                rowcount = rowcount - batchSize;
                batchNumber = batchNumber + 1;
            }
            return batches;
        }

        public static async Task<Dictionary<object,object>> get_query_execution_result(string query, IAmazonAthena client)
        {
            Console.WriteLine(query);
            var response_query_execution_id = await client.StartQueryExecutionAsync(new StartQueryExecutionRequest
            {
                QueryString = query,
                QueryExecutionContext = new QueryExecutionContext
                {
                    Database = "dvap-poc-db"
                },
                ResultConfiguration = new ResultConfiguration
                {
                    OutputLocation = "s3://dvap-poc/result"
                }
            }
                );
            var response_get_query_details = await client.GetQueryExecutionAsync(new GetQueryExecutionRequest
            {
                QueryExecutionId = response_query_execution_id.QueryExecutionId
            }
                );

            var status = "RUNNING";
            var iterations = 360;
            while (iterations > 0)
            {
                iterations = iterations - 1;
                response_get_query_details = await client.GetQueryExecutionAsync(new GetQueryExecutionRequest
                {
                    QueryExecutionId = response_query_execution_id.QueryExecutionId
                }
                    );

                status = response_get_query_details.QueryExecution.Status.State;
                if (status == "FAILED" || status == "CANCELLED")
                {
                    var failure_reason = response_get_query_details.QueryExecution.Status.StateChangeReason;
                    Console.WriteLine(failure_reason);
                    return new Dictionary<object, object> {
                    {
                        "status",
                        status}};
                }
                else if (status == "SUCCEEDED")
                {
                    var location = response_get_query_details.QueryExecution.ResultConfiguration.OutputLocation;
                    //# Function to get output results
                    var response_query_result = await client.GetQueryResultsAsync(new GetQueryResultsRequest
                    {
                        QueryExecutionId = response_query_execution_id.QueryExecutionId
                    });
                    var result_data = response_query_result.ResultSet;
                    return new Dictionary<object, object> {
                    {
                        "status",
                        status},
                    {
                        "result",
                        result_data}};
                }

                Thread.Sleep(1);

            }
            return new Dictionary<object, object> {
                    {
                        "status",
                        status}};
        }

        public string FunctionHandler(string input, ILambdaContext context)
        {
            var query = "SELECT COUNT(*) FROM \"dvap-poc-db\".\"input\";";
            var response_query_result = get_query_execution_result(query, AthenaClient);
            var countrow = 0;
            if (response_query_result["status"] == "SUCCEEDED")
            {
                if (response_query_result["result"]["Rows"].Count > 1)
                {
                    countrow = response_query_result["result"]["Rows"][1]["Data"][0]["VarCharValue"];
                    Console.WriteLine(countrow);
                }
            }
            return generate_batches(Convert.ToInt32(countrow));
        }
    }
}
