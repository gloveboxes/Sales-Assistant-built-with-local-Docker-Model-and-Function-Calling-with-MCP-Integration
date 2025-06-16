#!/usr/bin/env python3
"""
MCP Server with database tools for the docker model runner.
Provides comprehensive customer sales database access with individual table schema tools.
"""

from contextlib import asynccontextmanager
from collections.abc import AsyncIterator
from dataclasses import dataclass

from mcp.server.fastmcp import FastMCP

from sales_data_complex import DatabaseSchemaProvider
from utilities import Utilities

# Initialize utilities for path resolution
utilities = Utilities()
db_path = utilities.shared_files_path / "database" / "customer_sales.db"


@dataclass
class AppContext:
    """Application context containing database connection."""
    db: DatabaseSchemaProvider


@asynccontextmanager
async def app_lifespan(server: FastMCP) -> AsyncIterator[AppContext]:
    """Manage application lifecycle with type-safe context"""
    # Initialize on startup
    # print(f"🚀 Starting MCP server with database: {db_path}")
    
    # Check if database file exists
    if not db_path.exists():
        raise RuntimeError(f"Database file not found at {db_path}")
    
    db = DatabaseSchemaProvider(str(db_path))
    await db.open_connection()
    
    try:
        yield AppContext(db=db)
    finally:
        # Cleanup on shutdown
        try:
            await db.close_connection()
        except Exception as e:
            print(f"⚠️  Error closing database: {e}")


# Create MCP server with lifespan support
mcp = FastMCP("customer-sales-tools", lifespan=app_lifespan)


def get_db_provider() -> DatabaseSchemaProvider:
    """Get the database provider instance from context."""
    ctx = mcp.get_context()
    app_context = ctx.request_context.lifespan_context
    if isinstance(app_context, AppContext):
        return app_context.db
    else:
        raise RuntimeError("Invalid lifespan context type")


@mcp.tool()
async def get_customers_table_schema() -> str:
    """Get the complete schema information for the customers table. **ALWAYS call this tool first** when queries involve customer data, customer information, regions, or customer-related analysis. This provides table structure, available regions, column types, and relationships needed for accurate query generation."""
    try:
        provider = get_db_provider()
        schema_info = await provider.get_table_metadata_string("customers")
        return f"Customers Table Schema:\n\n{schema_info}"
    except Exception as e:
        return f"Error retrieving customers table schema: {str(e)}"


@mcp.tool()
async def get_products_table_schema() -> str:
    """Get the complete schema information for the products table. **ALWAYS call this tool first** when queries involve product data, product types, categories, or product-related analysis. This provides table structure, available product types, categories, column types, and relationships needed for accurate query generation."""
    try:
        provider = get_db_provider()
        schema_info = await provider.get_table_metadata_string("products")
        return f"Products Table Schema:\n\n{schema_info}"
    except Exception as e:
        return f"Error retrieving products table schema: {str(e)}"


@mcp.tool()
async def get_orders_table_schema() -> str:
    """Get the complete schema information for the orders table. **ALWAYS call this tool first** when queries involve order data, sales transactions, dates, or order-related analysis. This provides table structure, available years, column types, and relationships needed for accurate query generation."""
    try:
        provider = get_db_provider()
        schema_info = await provider.get_table_metadata_string("orders")
        return f"Orders Table Schema:\n\n{schema_info}"
    except Exception as e:
        return f"Error retrieving orders table schema: {str(e)}"


@mcp.tool()
async def execute_sales_query(sqlite_query: str) -> str:
    """Execute a SQLite query against the customer sales database.

CRITICAL WORKFLOW - ALWAYS FOLLOW THIS ORDER:
1. **FIRST**: Call the appropriate schema tool(s) based on your query needs:
   - get_customers_table_schema: For customer data, regions, customer analysis
   - get_products_table_schema: For product data, categories, product types
   - get_orders_table_schema: For order data, sales transactions, dates
2. **THEN**: Write your query using the exact table/column names from the schema
3. **FINALLY**: Execute the query with this tool

QUERY GUIDELINES:
- Default to aggregation (SUM, AVG, COUNT, GROUP BY) unless user requests details
- Always include LIMIT 20 in every query - never return more than 20 rows
- Use only valid table and column names from the schema
- Never return all rows from any table without aggregation
- Available tables: customers, products, orders
- Always join tables using the foreign key relationships shown in schemas
    
Args:
    sqlite_query: A well-formed SQLite query to extract sales data. Must include LIMIT 30.
    """
    try:
        if not sqlite_query:
            return "Error: sqlite_query parameter is required"

        # Validate that query includes LIMIT
        if "LIMIT" not in sqlite_query.upper():
            return "Error: Query must include 'LIMIT 20' to prevent returning too many rows. Please modify your query."

        provider = get_db_provider()
        result = await provider.execute_query(sqlite_query)
        return f"Query Results:\n{result}"

    except Exception as e:
        return f"Error executing database query: {str(e)}"


if __name__ == "__main__":
    # Run the FastMCP server
    mcp.run()
