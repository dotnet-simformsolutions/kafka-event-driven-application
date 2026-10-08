using System.ComponentModel.DataAnnotations;
using OrderFlow.Contracts;

namespace OrderFlow.Producer.Models;

/// <summary>
/// Request body for POST /api/orders.
///
/// The attributes below are the whole of the validation, enforced by the endpoint filter
/// that <c>builder.Services.AddValidation()</c> installs in Program.cs. That call is
/// load-bearing: minimal APIs, unlike <c>[ApiController]</c> MVC, do not validate
/// DataAnnotations implicitly, so removing it silently turns every rule here into a comment.
/// </summary>
public sealed class CreateOrderRequest
{
    /// <summary>
    /// Customer id. Use the literal <c>CUST-FAIL</c> to make the consumer's handler
    /// throw, which exercises the retry-then-skip path.
    /// </summary>
    [Required, StringLength(64, MinimumLength = 1)]
    public string CustomerId { get; set; } = string.Empty;

    [Required, StringLength(200, MinimumLength = 1)]
    public string CustomerName { get; set; } = string.Empty;

    [StringLength(3, MinimumLength = 3)]
    public string Currency { get; set; } = "USD";

    [Required, MinLength(1)]
    public List<OrderItemRequest> Items { get; set; } = [];

    public OrderCreatedEvent ToEvent(Guid orderId, string correlationId) => new()
    {
        OrderId = orderId,
        CorrelationId = correlationId,
        CustomerId = CustomerId,
        CustomerName = CustomerName,
        Currency = Currency,
        TotalAmount = Items.Sum(i => i.UnitPrice * i.Quantity),
        Items = [.. Items.Select(i => new OrderItem
        {
            ProductId = i.ProductId,
            ProductName = i.ProductName,
            Quantity = i.Quantity,
            UnitPrice = i.UnitPrice
        })]
    };
}

public sealed class OrderItemRequest
{
    [Required] public string ProductId { get; set; } = string.Empty;
    public string ProductName { get; set; } = string.Empty;
    [Range(1, 10_000)] public int Quantity { get; set; } = 1;
    [Range(0, 1_000_000)] public decimal UnitPrice { get; set; }
}
