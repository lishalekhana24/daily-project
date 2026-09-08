object SalesSummary {

  def main(args: Array[String]): Unit = {

    // Sales records
    val sales = List(
      ("Laptop", 2, 50000),
      ("Mouse", 5, 500),
      ("Keyboard", 3, 1500),
      ("Laptop", 1, 50000),
      ("Mouse", 2, 500)
    )

    // 1. map - calculate total amount for each sale
    val totals = sales.map {
      case (product, quantity, price) =>
        (product, quantity, quantity * price)
    }

    println("Sales with Total Amount:")
    println(totals)

    // 2. filter - find sales with quantity greater than 2
    val filteredSales = sales.filter {
      case (_, quantity, _) => quantity > 2
    }

    println("\nSales with Quantity > 2:")
    println(filteredSales)

    // 3. flatMap - create individual product units
    val products = sales.flatMap {
      case (product, quantity, _) =>
        List.fill(quantity)(product)
    }

    println("\nIndividual Products:")
    println(products)

    // 4. reduce - calculate total sales amount
    val totalSales = sales.map {
      case (_, quantity, price) => quantity * price
    }.reduce(_ + _)

    println("\nTotal Sales Amount:")
    println(totalSales)

    // 5. Vector - indexed customer records
    val customers = Vector(
      "Lisha",
      "Rahul",
      "Anu",
      "Kiran"
    )

    println("\nCustomer at index 2:")
    println(customers(2))

    // 6. Map - calculate product quantities
    val productQuantities = sales
      .groupBy(_._1)
      .map {
        case (product, records) =>
          product -> records.map(_._2).sum
      }

    println("\nProduct Quantities:")
    println(productQuantities)

    // Map - calculate product revenue
    val productRevenue = sales
      .groupBy(_._1)
      .map {
        case (product, records) =>
          product -> records.map {
            case (_, quantity, price) => quantity * price
          }.sum
      }

    println("\nProduct Revenue:")
    println(productRevenue)

    // 7. For-comprehension combining customers and orders
    val orders = List(
      ("Lisha", "Laptop"),
      ("Rahul", "Mouse"),
      ("Anu", "Keyboard")
    )

    val customerOrders = for {
      customer <- customers
      order <- orders
      if customer == order._1
    } yield (customer, order._2)

    println("\nCustomer Orders:")
    println(customerOrders)

    // 8. Daily sales summary
    println("\n===== DAILY SALES SUMMARY =====")
    println(s"Total Sales: ₹$totalSales")
    println(s"Product Quantities: $productQuantities")
    println(s"Product Revenue: $productRevenue")
  }
}
