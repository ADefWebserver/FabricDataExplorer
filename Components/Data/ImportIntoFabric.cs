using Azure.Storage;
using Azure.Storage.Blobs;
using Azure.Storage.Sas;
using Microsoft.Data.SqlClient;
using Parquet;
using Parquet.Rows;
using Parquet.Schema;
using System.Data;
using System.Text;
using System.Text.RegularExpressions;

public class ImportIntoFabric
{
    public string SqlQuery { get; set; }
    public string TableName { get; set; }
    public string DataWarehouseConnectionString { get; set; }
    public string SQLConnectionString { get; set; }
    public string StorageAccountName { get; set; }
    public string StorageAccountKey { get; set; }
    public string StorageContainer { get; set; }

    public ImportIntoFabric()
    {
        // set default values
        SqlQuery = "";
        TableName = "";
        DataWarehouseConnectionString = "";
        SQLConnectionString = "";
        StorageAccountName = "";
        StorageAccountKey = "";
        StorageContainer = "";
    }

    public async Task<string> ImportData()
    {
        string response = $"Import of {TableName} complete!";

        try
        {
            var databaseFields =
                ExtractFields(SqlQuery);

            var parquetSchema =
                new ParquetSchema(databaseFields.Select(f => new DataField<string>(f)).ToList());

            var parquetTable =
                new Parquet.Rows.Table(parquetSchema);

            using (var connection = new SqlConnection(SQLConnectionString))
            {
                connection.Open();
                using (var command = new SqlCommand(SqlQuery, connection))
                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        var row = new Row(
                            databaseFields.Select((f, i) => CleanValue(reader.GetValue(i)?.ToString()))
                            .ToArray());

                        parquetTable.Add(row);
                    }
                }
            }

            using var ms = new MemoryStream();
            await parquetTable.WriteAsync(ms);
            ms.Position = 0;

            var blobClient = new BlobContainerClient(
                $"DefaultEndpointsProtocol=https;" +
                $"AccountName={StorageAccountName};" +
                $"AccountKey={StorageAccountKey};" +
                $"EndpointSuffix=core.windows.net",
                StorageContainer);

            string fileName = Path.GetFileName(TableName) + ".parquet";
            var blob = blobClient.GetBlobClient($"{fileName}");

            await blob.UploadAsync(ms, true);

            await Task.Delay(5000);

            var sasToken = GenerateSasToken();

            var sqlScripts = GenerateSqlScripts(TableName, databaseFields, sasToken);

            using (var connection = new SqlConnection(DataWarehouseConnectionString))
            {
                connection.Open();
                foreach (var sql in sqlScripts)
                {
                    using var command = new SqlCommand(sql, connection);
                    await command.ExecuteNonQueryAsync();
                    await Task.Delay(5000); // To ensure operations are sequenced properly
                }
            }
        }
        catch (Exception ex)
        {
            response =
                $"Error! ImportData() - TableName: {TableName} - {ex.Message} - {ex.StackTrace ?? ""}";
        }

        return response;
    }

    private static string CleanValue(string value)
    {
        return string.IsNullOrEmpty(value)
            ? ""
            : Regex.Replace(value, @"[\r\n\t]", " ").Trim();
    }

    private string GenerateSasToken()
    {
        var sasBuilder = new AccountSasBuilder
        {
            Protocol = SasProtocol.Https,
            Services = AccountSasServices.Blobs,
            ResourceTypes = AccountSasResourceTypes.Container | AccountSasResourceTypes.Object,
            ExpiresOn = DateTimeOffset.UtcNow.AddHours(1)
        };
        sasBuilder.SetPermissions(AccountSasPermissions.Read | AccountSasPermissions.Write);

        return sasBuilder.ToSasQueryParameters(
            new StorageSharedKeyCredential(
                StorageAccountName,
                StorageAccountKey)).ToString();
    }

    private List<string> GenerateSqlScripts(string tableName, IList<string> fields, string sasToken)
    {
        var fieldSchema =
            string.Join(", ", fields.Select(f => $"[{f}] VARCHAR(4000)"));

        var filePath =
            $"https://{StorageAccountName}.blob.core.windows.net/{StorageContainer}/{tableName}.parquet";

        return new List<string>
        {
            $"IF OBJECT_ID('[dbo].{tableName}', 'U') IS NOT NULL DROP TABLE dbo.{tableName}",
            AddNotNULL($"CREATE TABLE [dbo].{tableName} ({fieldSchema})"),
            $"ALTER TABLE [dbo].{tableName} ADD CONSTRAINT PK_{StripBrackets(tableName)} PRIMARY KEY NONCLUSTERED (_Id) NOT ENFORCED;",
            $"COPY INTO [dbo].{tableName} " +
            $"FROM '{filePath}' " +
            $"WITH (file_type = 'PARQUET', CREDENTIAL=(IDENTITY= 'Shared Access Signature', SECRET='{sasToken}'))"
        };
    }

    public static IList<string> ExtractFields(string sql)
    {
        // Start from: "SELECT ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS _Id," 
        sql = sql.Substring(52);

        // Replace _Id with [_Id]
        sql = sql.Replace("_Id", "[_Id]");

        // End at "from"        
        sql = sql.Substring(0, sql.IndexOf("from ", StringComparison.OrdinalIgnoreCase));

        return Regex.Matches(sql, @"\[([^\]]+)\]").Cast<Match>().Select(m => m.Groups[1].Value).ToList();
    }

    public static string AddNotNULL(string updateStatement)
    {
        // Add NOT NULL to [_Id] VARCHAR(4000) so Primary Key can be created
        return updateStatement.Replace("[_Id] VARCHAR(4000)", "[_Id] VARCHAR(4000) NOT NULL");
    }

    public static string StripBrackets(string tableName)
    {
        return tableName.Replace("[", "").Replace("]", "");
    }
}