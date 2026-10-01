using System.Text.Json;
using Microsoft.Data.Sqlite;
using OMS.Worker.Models;

namespace OMS.Worker.Services;

public sealed class SqliteOrderRepository : IOrderRepository
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower
    };

    private readonly string connectionString;

    public SqliteOrderRepository(string databasePath)
    {
        var fullPath = Path.GetFullPath(databasePath);
        Directory.CreateDirectory(Path.GetDirectoryName(fullPath)!);
        connectionString = new SqliteConnectionStringBuilder
        {
            DataSource = fullPath,
            Mode = SqliteOpenMode.ReadWriteCreate,
            Cache = SqliteCacheMode.Shared
        }.ToString();

        using var connection = OpenConnection();
        using var command = connection.CreateCommand();
        command.CommandText = """
            CREATE TABLE IF NOT EXISTS order_status (
                order_id    TEXT PRIMARY KEY,
                status      INTEGER NOT NULL,
                message     TEXT NULL,
                rrn         TEXT NULL,
                customer_id TEXT NULL,
                items_json  TEXT NULL,
                updated_at  TEXT NOT NULL
            );
            """;
        command.ExecuteNonQuery();
    }

    public void Save(OrderStatusView order)
    {
        using var connection = OpenConnection();
        using var command = connection.CreateCommand();
        command.CommandText = """
            INSERT INTO order_status (order_id, status, message, rrn, customer_id, items_json, updated_at)
            VALUES ($orderId, $status, $message, $rrn, $customerId, $itemsJson, $updatedAt)
            ON CONFLICT(order_id) DO UPDATE SET
                status      = excluded.status,
                message     = excluded.message,
                rrn         = excluded.rrn,
                customer_id = excluded.customer_id,
                items_json  = excluded.items_json,
                updated_at  = excluded.updated_at;
            """;
        command.Parameters.AddWithValue("$orderId",    order.OrderId);
        command.Parameters.AddWithValue("$status",     (int)order.Status);
        command.Parameters.AddWithValue("$message",    (object?)order.Message    ?? DBNull.Value);
        command.Parameters.AddWithValue("$rrn",        (object?)order.Rrn        ?? DBNull.Value);
        command.Parameters.AddWithValue("$customerId", (object?)order.CustomerId ?? DBNull.Value);
        command.Parameters.AddWithValue("$itemsJson",
            order.Items is { Count: > 0 }
                ? JsonSerializer.Serialize(order.Items, JsonOptions)
                : (object)DBNull.Value);
        command.Parameters.AddWithValue("$updatedAt",  DateTimeOffset.UtcNow.ToString("O"));
        command.ExecuteNonQuery();
    }

    public OrderStatusView? Get(string orderId)
    {
        using var connection = OpenConnection();
        using var command = connection.CreateCommand();
        command.CommandText = """
            SELECT status, message, rrn, customer_id, items_json
            FROM order_status
            WHERE order_id = $orderId;
            """;
        command.Parameters.AddWithValue("$orderId", orderId);

        using var reader = command.ExecuteReader();
        if (!reader.Read())
        {
            return null;
        }

        IReadOnlyList<OrderItem>? items = null;
        if (!reader.IsDBNull(4))
        {
            items = JsonSerializer.Deserialize<List<OrderItem>>(
                reader.GetString(4), JsonOptions);
        }

        return new OrderStatusView(
            OrderId:    orderId,
            Status:     (OrderStatus)reader.GetInt32(0),
            Message:    reader.IsDBNull(1) ? null : reader.GetString(1),
            Rrn:        reader.IsDBNull(2) ? null : reader.GetString(2),
            CustomerId: reader.IsDBNull(3) ? null : reader.GetString(3),
            Items:      items);
    }

    private SqliteConnection OpenConnection()
    {
        var connection = new SqliteConnection(connectionString);
        connection.Open();
        return connection;
    }
}
