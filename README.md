1. Extract Data
2.  start by writing a function to read the Excel files and extract the data. We will assume you can download the files manually from SharePoint (or use an API to automate it). For now, we'll focus on extracting data from local CSVs.

3. Transform Data
We will apply the necessary business rules:

Combine the data from both regions into a single table.
Add calculated columns such as total_sales and net_sale.
Remove duplicate OrderId values.
Filter out records with negative or zero net_sale.
3. Load Data into SQLite Database
We will create an SQLite database and table, then insert the transformed data into it.

4. Write SQL Queries to Validate Data
We will write SQL queries to:

Count the total number of records.
Find the total sales by region.
Find the average sales amount per transaction.
Ensure there are no duplicate OrderId values
