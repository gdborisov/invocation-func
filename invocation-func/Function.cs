using Amazon;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using Amazon.Lambda.Core;

// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.SystemTextJson.DefaultLambdaJsonSerializer))]

namespace invocation_func;

public class Function
{
    /// <summary>
    /// A simple function that takes a string and does a ToUpper
    /// </summary>
    /// <param name="input">The event for the Lambda function handler to process.</param>
    /// <param name="context">The ILambdaContext that provides methods for logging and describing the Lambda environment.</param>
    /// <returns></returns>
    public async Task<string> FunctionHandlerAsync(string input, ILambdaContext context)
    {
        var dynamo = new AmazonDynamoDBClient(RegionEndpoint.EUNorth1);
        var tableName = "Users";

        // Extracted table creation into its own method
        await CreateUsersTableIfNotExistsAsync(dynamo, tableName);

        // -------------------------------
        // TESTING
        // -------------------------------

        // Use the extracted helper method
        await PutUserAsync(dynamo, tableName, new UserProfile
        {
            UserId = "user123",
            Email = "test@example.com",
            Age = 30
        });

        // Use the extracted helper method
        var user = await GetUserAsync(dynamo, tableName, "user123");
        Console.WriteLine($"From DB: {user.Email}, age {user.Age}");

        // Use the new extracted helper method for deletion
        await DeleteUserAsync(dynamo, tableName, "user123");
        Console.WriteLine("Done.");

        return input.ToUpper();
    }

    // New extracted helper method for creating a user
    private static async Task PutUserAsync(IAmazonDynamoDB dynamo, string tableName, UserProfile user)
    {
        var request = new PutItemRequest
        {
            TableName = tableName,
            Item = new Dictionary<string, AttributeValue>
            {
                ["UserId"] = new AttributeValue { S = user.UserId },
                ["Email"] = new AttributeValue { S = user.Email },
                ["Age"] = new AttributeValue { N = user.Age.ToString() }
            }
        };

        await dynamo.PutItemAsync(request);
        Console.WriteLine("User created.");
    }

    // New helper method extracted from FunctionHandlerAsync
    private static async Task CreateUsersTableIfNotExistsAsync(IAmazonDynamoDB dynamo, string tableName)
    {
        var request = new CreateTableRequest
        {
            TableName = tableName,
            AttributeDefinitions = new List<AttributeDefinition>
            {
                new AttributeDefinition
                {
                    AttributeName = "UserId",
                    AttributeType = "S"
                }
            },
            KeySchema = new List<KeySchemaElement>
            {
                new KeySchemaElement
                {
                    AttributeName = "UserId",
                    KeyType = "HASH" // Partition key
                }
            },

            // Use PAY_PER_REQUEST so you don't need provisioning
            BillingMode = BillingMode.PAY_PER_REQUEST
        };

        try
        {
            var response = await dynamo.CreateTableAsync(request);
            Console.WriteLine($"Table creation initiated: {response.TableDescription.TableName}");
        }
        catch (ResourceInUseException)
        {
            Console.WriteLine($"Table '{tableName}' already exists.");
        }
    }

    // Extracted GetUser into a reusable private helper
    private static async Task<UserProfile?> GetUserAsync(IAmazonDynamoDB dynamo, string tableName, string userId)
    {
        var response = await dynamo.GetItemAsync(tableName,
            new Dictionary<string, AttributeValue>
            {
                ["UserId"] = new AttributeValue { S = userId }
            });

        if (response.Item == null || response.Item.Count == 0)
            return null;

        return new UserProfile
        {
            UserId = response.Item["UserId"].S,
            Email = response.Item["Email"].S,
            Age = int.Parse(response.Item["Age"].N)
        };
    }

    // Extracted DeleteUser as a reusable private helper
    private static async Task DeleteUserAsync(IAmazonDynamoDB dynamo, string tableName, string userId)
    {
        await dynamo.DeleteItemAsync(new DeleteItemRequest
        {
            TableName = tableName,
            Key = new Dictionary<string, AttributeValue>
            {
                ["UserId"] = new AttributeValue { S = userId }
            }
        });

        Console.WriteLine("User deleted.");
    }

    private static async Task QueryUserAsync(string userId)
    {
        var client = new AmazonDynamoDBClient();

        //var request = new QueryRequest
        //{
        //    TableName = "Users",
        //    KeyConditionExpression = "UserId = :uid",
        //    ExpressionAttributeValues = new Dictionary<string, AttributeValue>
        //{
        //    { ":uid", new AttributeValue { S = userId } }
        //}
        //};
        var request = new QueryRequest
        {
            TableName = "Users",
            KeyConditionExpression = "UserId = :uid and CreatedAt >= :ts",
            ExpressionAttributeValues = new Dictionary<string, AttributeValue>
            {
                { ":uid", new AttributeValue { S = userId } },
                { ":ts", new AttributeValue { N = "1700000000" } } // example timestamp
            }
        };


        var response = await client.QueryAsync(request);

        Console.WriteLine($"Query returned {response.Count} items.");

        foreach (var item in response.Items)
        {
            Console.WriteLine($"Item: {string.Join(", ", item.Select(kv => $"{kv.Key} = {kv.Value}"))}");
        }
    }

    private static async Task ScanActiveUsersAsync()
    {
        var client = new AmazonDynamoDBClient();

        var request = new ScanRequest
        {
            TableName = "Users",
            FilterExpression = "IsActive = :active",
            ExpressionAttributeValues = new Dictionary<string, AttributeValue>
        {
            { ":active", new AttributeValue { BOOL = true } }
        }
        };

        var response = await client.ScanAsync(request);

        Console.WriteLine($"Scan returned {response.Count} items.");

        foreach (var item in response.Items)
        {
            Console.WriteLine($"Item: {string.Join(", ", item.Select(kv => $"{kv.Key} = {kv.Value}"))}");
        }
    }

}
