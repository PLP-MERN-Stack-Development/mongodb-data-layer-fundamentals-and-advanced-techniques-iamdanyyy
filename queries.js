const { MongoClient } = require("mongodb");

const uri = "mongodb://127.0.0.1:27017";
const client = new MongoClient(uri);

async function run() {
  try {
    await client.connect();
    const db = client.db("plp_bookstore");
    const books = db.collection("books");

    console.log("Connected to MongoDB");

    // Find all books in a specific genre
    const genreBooks = await books.find({ genre: "Fiction" }).toArray();
    console.log("Books in Fiction genre:", genreBooks);

    // Find books published after a certain year
    const recentBooks = await books.find({ published_year: { $gt: 1980 } }).toArray();
    console.log("Books published after 1980:", recentBooks);

    // Find books by a specific author
    const authorBooks = await books.find({ author: "George Orwell" }).toArray();
    console.log("Books by George Orwell:", authorBooks);

    // Update the price of a specific book
    const updateResult = await books.updateOne(
      { title: "The Lord of the Rings" },
      { $set: { price: 25.99 } }
    );
    console.log("Updated book price:", updateResult.modifiedCount);

    // Delete a book by its title
    const deleteResult = await books.deleteOne({ title: "The Alchemist" });
    console.log("Deleted book:", deleteResult.deletedCount);

    // Books in stock and published after 2010
    const inStockRecent = await books.find({
      in_stock: true,
      published_year: { $gt: 2010 },
    }).toArray();
    console.log("In stock & published after 2010:", inStockRecent);

    // Projection (only return title, author, price)
    const projection = await books.find({}, { projection: { title: 1, author: 1, price: 1 } }).toArray();
    console.log("Projection (title, author, price):", projection);

    // Sorting by price
    const sortedAsc = await books.find().sort({ price: 1 }).toArray();
    const sortedDesc = await books.find().sort({ price: -1 }).toArray();
    console.log("Books sorted by price ascending:", sortedAsc);
    console.log("Books sorted by price descending:", sortedDesc);

    // Pagination (5 books per page, page 1)
    const page1 = await books.find().skip(0).limit(5).toArray();
    const page2 = await books.find().skip(5).limit(5).toArray();
    console.log("Page 1:", page1);
    console.log("Page 2:", page2);

    // Average price by genre
    const avgPriceByGenre = await books.aggregate([
      { $group: { _id: "$genre", avgPrice: { $avg: "$price" } } }
    ]).toArray();
    console.log("Average price by genre:", avgPriceByGenre);

    // Author with most books
    const topAuthor = await books.aggregate([
      { $group: { _id: "$author", count: { $sum: 1 } } },
      { $sort: { count: -1 } },
      { $limit: 1 }
    ]).toArray();
    console.log("Author with most books:", topAuthor);

    // Group books by publication decade
    const booksByDecade = await books.aggregate([
        {
            $group: {
            _id: {
                $concat: [
                    { $toString: { $multiply: [{ $floor: { $divide: ["$published_year", 10] } }, 10] } }, 
                    "s"
                ]
            },
            count: { $sum: 1 }
            }
        },
        { $sort: { _id: 1 } }
    ]).toArray();
    console.log("Books grouped by decade:", booksByDecade);

    // Index on title
    await books.createIndex({ title: 1 });

    // Compound index on author + published_year
    await books.createIndex({ author: 1, published_year: -1 });

    // Explain query with index
    const explainResult = await books.find({ title: "Sample Book" }).explain("executionStats");
    console.log("Explain output for index usage:", explainResult.executionStats);

  } finally {
    await client.close();
    console.log("Connection closed");
  }
}

run().catch(console.dir);