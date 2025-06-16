#!/usr/bin/env python3
"""
MCP Server with database tools for the docker model runner.
Provides comprehensive customer sales database access with individual table schema tools.
"""

import asyncio
from typing import Any, Dict, List

from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp.types import Tool, TextContent

from sales_data_complex import DatabaseSchemaProvider
from utilities import Utilities


# Initialize the MCP server
server = Server("customer-sales-tools")

# Initialize database connection
utilities = Utilities()
db_path = utilities.shared_files_path / "database" / "customer_sales.db"


@server.list_tools()
async def list_tools() -> List[Tool]:
    """List available tools."""
    return [
        Tool(
            name="get_customers_table_schema",
            description="Get the complete schema information for the customers table. **ALWAYS call this tool first** when queries involve customer data, customer information, regions, or customer-related analysis. This provides table structure, available regions, column types, and relationships needed for accurate query generation.",
            inputSchema={"type": "object", "properties": {}, "required": []},
        ),
        Tool(
            name="get_products_table_schema",
            description="Get the complete schema information for the products table. **ALWAYS call this tool first** when queries involve product data, product types, categories, or product-related analysis. This provides table structure, available product types, categories, column types, and relationships needed for accurate query generation.",
            inputSchema={"type": "object", "properties": {}, "required": []},
        ),
        Tool(
            name="get_orders_table_schema",
            description="Get the complete schema information for the orders table. **ALWAYS call this tool first** when queries involve order data, sales transactions, dates, or order-related analysis. This provides table structure, available years, column types, and relationships needed for accurate query generation.",
            inputSchema={"type": "object", "properties": {}, "required": []},
        ),
        Tool(
            name="execute_sales_query",
            description="""Execute a SQLite query against the customer sales database.

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
- Always join tables using the foreign key relationships shown in schemas""",
            inputSchema={
                "type": "object",
                "properties": {
                    "sqlite_query": {
                        "type": "string",
                        "description": "A well-formed SQLite query to extract sales data. Must include LIMIT 30.",
                    }
                },
                "required": ["sqlite_query"],
            },
        ),
    ]


async def handle_get_customers_table_schema(arguments: Dict[str, Any]) -> List[TextContent]:
    """Handle get_customers_table_schema tool calls."""
    try:
        async with DatabaseSchemaProvider(str(db_path)) as provider:
            schema_info = await provider.get_table_metadata_string("customers")
            return [
                TextContent(
                    type="text", text=f"Customers Table Schema:\n\n{schema_info}"
                )
            ]
    except Exception as e:
        return [
            TextContent(
                type="text", text=f"Error retrieving customers table schema: {str(e)}"
            )
        ]


async def handle_get_products_table_schema(arguments: Dict[str, Any]) -> List[TextContent]:
    """Handle get_products_table_schema tool calls."""
    try:
        async with DatabaseSchemaProvider(str(db_path)) as provider:
            schema_info = await provider.get_table_metadata_string("products")
            return [
                TextContent(
                    type="text", text=f"Products Table Schema:\n\n{schema_info}"
                )
            ]
    except Exception as e:
        return [
            TextContent(
                type="text", text=f"Error retrieving products table schema: {str(e)}"
            )
        ]


async def handle_get_orders_table_schema(arguments: Dict[str, Any]) -> List[TextContent]:
    """Handle get_orders_table_schema tool calls."""
    try:
        async with DatabaseSchemaProvider(str(db_path)) as provider:
            schema_info = await provider.get_table_metadata_string("orders")
            return [
                TextContent(
                    type="text", text=f"Orders Table Schema:\n\n{schema_info}"
                )
            ]
    except Exception as e:
        return [
            TextContent(
                type="text", text=f"Error retrieving orders table schema: {str(e)}"
            )
        ]


async def handle_execute_sales_query(arguments: Dict[str, Any]) -> List[TextContent]:
    """Handle execute_sales_query tool calls."""
    try:
        sqlite_query = arguments.get("sqlite_query")

        if not sqlite_query:
            return [
                TextContent(
                    type="text", text="Error: sqlite_query parameter is required"
                )
            ]

        # Validate that query includes LIMIT
        if "LIMIT" not in sqlite_query.upper():
            return [
                TextContent(
                    type="text",
                    text="Error: Query must include 'LIMIT 30' to prevent returning too many rows. Please modify your query.",
                )
            ]

        async with DatabaseSchemaProvider(str(db_path)) as provider:
            result = await provider.execute_query(sqlite_query)
            return [TextContent(type="text", text=f"Query Results:\n{result}")]

    except Exception as e:
        return [
            TextContent(
                type="text", text=f"Error executing database query: {str(e)}"
            )
        ]


# Tool handler registry - maps tool names to their handler functions
TOOL_HANDLERS = {
    "get_customers_table_schema": handle_get_customers_table_schema,
    "get_products_table_schema": handle_get_products_table_schema,
    "get_orders_table_schema": handle_get_orders_table_schema,
    "execute_sales_query": handle_execute_sales_query,
}


@server.call_tool()
async def call_tool(name: str, arguments: Dict[str, Any]) -> List[TextContent]:
    """Handle tool calls using a registry of handler functions."""

    # Check if database file exists
    if not db_path.exists():
        return [
            TextContent(
                type="text",
                text=f"Error: Database file not found at {db_path}. Please ensure the customer sales database exists.",
            )
        ]

    # Look up the handler function for this tool
    handler = TOOL_HANDLERS.get(name)
    
    if handler is None:
        return [TextContent(type="text", text=f"Unknown tool: {name}")]
    
    # Call the handler function
    return await handler(arguments)


async def main():
    """Main entry point for the MCP server."""
    async with stdio_server() as streams:
        await server.run(*streams, server.create_initialization_options())


if __name__ == "__main__":
    asyncio.run(main())
