#!/usr/bin/env python3
"""
AI-Friendly Database Schema Tool

This script provides methods to query database table schemas in AI-friendly formats
for dynamic query generation and AI model integration.

Usage:
    python test_sqlite.py

Requirements:
    - aiosqlite (async SQLite adapter)
    - asyncio (for async operations)
    - pandas (for structured JSON output)
"""

import asyncio
import aiosqlite
import json
import logging
import pandas as pd
from pathlib import Path
from typing import Dict, List, Any, Optional

# Configure logging
logging.basicConfig(level=logging.ERROR)
logger = logging.getLogger(__name__)

# Constants
CUSTOMERS_TABLE = "customers"
PRODUCTS_TABLE = "products"
ORDERS_TABLE = "orders"


class DatabaseSchemaProvider:
    """Provides database schema information in AI-friendly formats for dynamic query generation."""

    def __init__(self, db_path: str):
        self.db_path = db_path
        self.connection: Optional[aiosqlite.Connection] = None
        self.all_schemas: Optional[Dict[str, Dict[str, Any]]] = None

    async def __aenter__(self):
        """Async context manager entry - just return self, don't auto-open connection."""
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit - close connection if it was opened."""
        await self.close_connection()

    async def open_connection(self):
        """Open SQLite connection and preload schemas."""
        if self.connection is None:
            self.connection = await aiosqlite.connect(self.db_path)
            self.all_schemas = await self.get_all_schemas()
            # print(f"✅ Database connection opened: {self.db_path}")

    async def close_connection(self):
        """Close SQLite connection and cleanup."""
        if self.connection:
            await self.connection.close()
            self.connection = None
            self.all_schemas = None
            print("✅ Database connection closed")

    async def table_exists(self, table: str) -> bool:
        """Check if a table exists in the database."""
        if not self.connection:
            return False
        async with self.connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name = ?", (table,)
        ) as cursor:
            return await cursor.fetchone() is not None

    async def column_exists(self, table: str, column: str) -> bool:
        """Check if a column exists in the given table."""
        if not self.connection:
            return False
        async with self.connection.execute(f"PRAGMA table_info({table})") as cursor:
            columns = [row[1] for row in await cursor.fetchall()]
            return column in columns

    async def fetch_distinct_values(self, column: str, table: str) -> List[str]:
        """Return sorted list of distinct values for a given column in a table, after validation."""
        if not self.connection:
            raise ValueError("Database connection not established")
        if not await self.table_exists(table):
            raise ValueError(f"Table '{table}' does not exist in the database")
        if not await self.column_exists(table, column):
            raise ValueError(f"Column '{column}' does not exist in table '{table}'")

        async with self.connection.execute(
            f"SELECT DISTINCT {column} FROM {table} WHERE {column} IS NOT NULL ORDER BY {column}"
        ) as cursor:
            return [row[0] for row in await cursor.fetchall() if row[0]]

    def infer_relationship_type(self, references_table: str) -> str:
        """Infer a relationship type based on the referenced table."""
        return (
            "many_to_one"
            if references_table in {CUSTOMERS_TABLE, PRODUCTS_TABLE}
            else "one_to_many"
        )

    async def get_table_schema(self, table_name: str) -> Dict[str, Any]:
        """Return schema information for a given table."""
        if not self.connection:
            return {"error": "Database connection not established"}

        if not await self.table_exists(table_name):
            return {"error": f"Table '{table_name}' not found"}

        async with self.connection.execute(
            f"PRAGMA table_info({table_name})"
        ) as columns_cursor:
            columns = await columns_cursor.fetchall()

        async with self.connection.execute(
            f"PRAGMA foreign_key_list({table_name})"
        ) as foreign_keys_cursor:
            foreign_keys = await foreign_keys_cursor.fetchall()

        columns_format = ", ".join(f"{col[1]}:{col[2]}" for col in columns)
        lower_table = table_name.lower()

        enum_queries = {
            CUSTOMERS_TABLE: {"available_regions": ("region", CUSTOMERS_TABLE)},
            PRODUCTS_TABLE: {
                "available_product_types": ("product_type", PRODUCTS_TABLE),
                "available_product_categories": ("main_category", PRODUCTS_TABLE),
            },
        }

        enum_data = {}
        if lower_table in {CUSTOMERS_TABLE, PRODUCTS_TABLE}:
            for key, (column, table) in enum_queries.get(lower_table, {}).items():
                try:
                    enum_data[key] = await self.fetch_distinct_values(column, table)
                except Exception:
                    enum_data[key] = []

        reporting_years = None
        if lower_table == ORDERS_TABLE:
            async with self.connection.execute(
                "SELECT DISTINCT strftime('%Y', order_date) as year FROM orders "
                "WHERE order_date IS NOT NULL ORDER BY year"
            ) as cursor:
                years = [row[0] for row in await cursor.fetchall() if row[0]]
                if years:
                    reporting_years = ", ".join(years)

        schema_data = {
            "table_name": table_name,
            "description": f"Table containing {table_name} data",
            "columns_format": columns_format,
            "columns": [
                {
                    "name": col[1],
                    "type": col[2],
                    "primary_key": bool(col[5]),
                    "required": bool(col[3]),
                    "default_value": col[4],
                }
                for col in columns
            ],
            "foreign_keys": [
                {
                    "column": fk[3],
                    "references_table": fk[2],
                    "references_column": fk[4],
                    "description": f"{fk[3]} links to {fk[2]}.{fk[4]}",
                    "relationship_type": self.infer_relationship_type(fk[2]),
                }
                for fk in foreign_keys
            ],
        }

        schema_data.update(enum_data)
        if reporting_years:
            schema_data["reporting_years"] = reporting_years

        return schema_data

    async def get_all_table_names(self) -> List[str]:
        """Get all user-defined table names in the database."""
        if not self.connection:
            return []
        async with self.connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
        ) as cursor:
            return [table[0] for table in await cursor.fetchall()]

    async def get_all_schemas(self) -> Dict[str, Dict[str, Any]]:
        """Get schema metadata for all tables."""
        table_names = await self.get_all_table_names()
        result = {}
        for table_name in table_names:
            result[table_name] = await self.get_table_schema(table_name)
        return result

    def format_schema_metadata_for_ai(self, schema: Dict[str, Any]) -> str:
        """Format schema data into an AI-readable format."""
        if "error" in schema:
            return f"**ERROR:** {schema['error']}"

        lines = [f"# Table: {schema['table_name']}", ""]
        lines.append(
            f"**Purpose:** {schema.get('description', 'No description available')}"
        )
        lines.append("\n## Schema")
        lines.append(schema.get("columns_format", "N/A"))

        if schema.get("foreign_keys"):
            lines.append("\n## Relationships")
            for fk in schema["foreign_keys"]:
                lines.append(
                    f"- `{fk['column']}` → `{fk['references_table']}.{fk['references_column']}` ({fk['relationship_type'].upper()})"
                )

        enum_fields = [
            ("available_regions", "Valid Regions"),
            ("available_product_types", "Valid Product Types"),
            ("available_product_categories", "Valid Categories"),
            ("reporting_years", "Available Years"),
        ]

        enum_lines = []
        for field_key, label in enum_fields:
            if field_key in schema and schema[field_key]:
                values = schema[field_key]
                if isinstance(values, list) and len(values) > 10:
                    enum_lines.append(
                        f"**{label}:** {', '.join(values[:10])}, ... [{len(values)} total options]"
                    )
                else:
                    enum_lines.append(
                        f"**{label}:** {', '.join(values) if isinstance(values, list) else values}"
                    )

        if enum_lines:
            lines.append("\n## Valid Values")
            lines.extend(enum_lines)

        lines.append("\n## Query Hints")
        lines.append(
            f"- Use `{schema['table_name']}` for queries about {schema['table_name'].replace('_', ' ')}"
        )
        if schema.get("foreign_keys"):
            for fk in schema["foreign_keys"]:
                lines.append(
                    f"- Join with `{fk['references_table']}` using `{fk['column']}`"
                )

        return "\n".join(lines) + "\n"

    async def get_table_metadata_string(self, table_name: str) -> str:
        """Return formatted schema metadata string for a single table."""
        if self.all_schemas and table_name in self.all_schemas:
            return self.format_schema_metadata_for_ai(self.all_schemas[table_name])
        schema = await self.get_table_schema(table_name)
        return self.format_schema_metadata_for_ai(schema)

    async def execute_query(self, sqlite_query: str) -> str:
        """Execute a SQL query and return results as JSON."""
        if not self.connection:
            return json.dumps({"error": "Database connection not established"})

        # logger.info(f"\n🔍 Executing SQLite query: {sqlite_query}\n")

        try:
            async with self.connection.execute(sqlite_query) as cursor:
                rows = await cursor.fetchall()
                columns = [description[0] for description in cursor.description]

            if not rows:
                return json.dumps(
                    "The query returned no results. Try a different question."
                )

            data = pd.DataFrame(rows, columns=columns)
            return data.to_json(index=False, orient="split")

        except Exception as e:
            return json.dumps(
                {"SQLite query failed with error": str(e), "query": sqlite_query}
            )


async def main():
    """Main function to run the schema tool."""
    db_path = Path(__file__).parent / "shared" / "database" / "customer_sales.db"

    if not db_path.exists():
        logger.error(f"❌ Error: Database file not found at {db_path}")
        logger.error("   Please run the database generator first:")
        logger.error("   python shared/database/data-generator/generate_customer_db.py")
        return

    logger.info("🤖 AI-Friendly Database Schema Tool")
    logger.info("=" * 50)

    try:
        async with DatabaseSchemaProvider(str(db_path)) as provider:
            # Explicitly open the database connection when needed
            await provider.open_connection()
            
            logger.info("\n📋 Getting all table schemas...")
            if not provider.all_schemas:
                logger.warning("❌ No schemas available")
                return

            logger.info("\n🧪 Testing SQL Query Execution:")
            logger.info("=" * 50)

            logger.info("\n📊 Test 1: Count all customers")
            result = await provider.execute_query(
                "SELECT COUNT(*) as total_customers FROM customers"
            )
            logger.info(f"Result: {result}")

            logger.info("\n✅ SQL Query tests completed!")
            logger.info("=" * 50)
            print("\n📋 All table schemas:\n")

            # --- Use print for clean, user-facing schema info ---
            print(await provider.get_table_metadata_string(CUSTOMERS_TABLE))
            print(await provider.get_table_metadata_string(PRODUCTS_TABLE))
            print(await provider.get_table_metadata_string(ORDERS_TABLE))
            print(await provider.get_table_metadata_string("non_existing_table"))

    except Exception as e:
        logger.error(f"❌ Error during analysis: {e}")
        raise


if __name__ == "__main__":
    asyncio.run(main())
