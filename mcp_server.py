#!/usr/bin/env python3
"""
MCP Server with math and database tools for the docker model runner.
Provides math tools and Contoso sales database access.
"""
import asyncio
from typing import Any, Dict, List

from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp.types import Tool, TextContent

from sales_data import SalesData
from utilities import Utilities


# Initialize the MCP server
server = Server("contoso-sales-tools")

# Initialize database connection
utilities = Utilities()
sales_data = SalesData(utilities)


@server.list_tools()
async def list_tools() -> List[Tool]:
    """List available tools."""
    return [
        Tool(
            name="get_database_schema",
            description="Get the database schema information for the Contoso Sales Database. Always call this tool first before generating SQL queries to understand the available tables, columns, and data values.",
            inputSchema={
                "type": "object",
                "properties": {},
                "required": []
            }
        ),
        Tool(
            name="fetch_sales_data_using_sqlite_query",
            description="""Query the Contoso Sales Database using SQLite. 

IMPORTANT GUIDELINES:
- Always call get_database_schema FIRST to understand the database structure
- Default to aggregation (SUM, AVG, COUNT, GROUP BY) unless user requests details
- Treat 'sales' and 'revenue' as synonyms for the Revenue column
- Always include LIMIT 30 in every query - never return more than 30 rows
- Use only valid table and column names from the schema
- Never return all rows from any table without aggregation""",
            inputSchema={
                "type": "object",
                "properties": {
                    "sqlite_query": {
                        "type": "string",
                        "description": "A well-formed SQLite query to extract sales data. Must include LIMIT 30."
                    }
                },
                "required": ["sqlite_query"]
            }
        )
    ]


@server.call_tool()
async def call_tool(name: str, arguments: Dict[str, Any]) -> List[TextContent]:
    """Handle tool calls."""
    
    # Ensure database connection is established
    if sales_data.conn is None:
        await sales_data.connect()
        if sales_data.conn is None:
            return [
                TextContent(
                    type="text",
                    text="Error: Unable to connect to the Contoso Sales Database"
                )
            ]
    
    if name == "get_database_schema":
        try:
            schema_info = await sales_data.get_database_info()
            return [
                TextContent(
                    type="text",
                    text=f"Contoso Sales Database Schema:\n\n{schema_info}"
                )
            ]
        except Exception as e:
            return [
                TextContent(
                    type="text",
                    text=f"Error retrieving database schema: {str(e)}"
                )
            ]
    
    elif name == "fetch_sales_data_using_sqlite_query":
        try:
            sqlite_query = arguments.get("sqlite_query")
            
            if not sqlite_query:
                return [
                    TextContent(
                        type="text",
                        text="Error: sqlite_query parameter is required"
                    )
                ]
            
            # Validate that query includes LIMIT
            if "LIMIT" not in sqlite_query.upper():
                return [
                    TextContent(
                        type="text",
                        text="Error: Query must include 'LIMIT 30' to prevent returning too many rows. Please modify your query."
                    )
                ]
            
            result = await sales_data.async_fetch_sales_data_using_sqlite_query(sqlite_query)
            
            return [
                TextContent(
                    type="text",
                    text=f"Query Results:\n{result}"
                )
            ]
            
        except Exception as e:
            return [
                TextContent(
                    type="text",
                    text=f"Error executing database query: {str(e)}"
                )
            ]
    
    else:
        return [
            TextContent(
                type="text",
                text=f"Unknown tool: {name}"
            )
        ]


async def main():
    """Main entry point for the MCP server."""
    try:
        async with stdio_server() as streams:
            await server.run(*streams, server.create_initialization_options())
    finally:
        # Ensure database connection is closed
        if sales_data.conn:
            await sales_data.close()


if __name__ == "__main__":
    asyncio.run(main())
