#!/usr/bin/env python3
"""
AI-Friendly Database Schema Tool

This script provides methods to query database table schemas in AI-friendly formats
for dynamic query genera        return schemas

    def format_schema_metadata_for_ai(self, schema: Dict[str, Any]) -> str:I model integration.

Usage:
    python test_sqlite.py

Requirements:
    - aiosqlite (async SQLite adapter)
    - asyncio (for async operations)
"""

import asyncio
import aiosqlite
import json
import pandas as pd
from pathlib import Path
from typing import Dict, List, Any, Optional, Union


class DatabaseSchemaProvider:
    """Provides database schema information in AI-friendly formats for dynamic query generation."""

    def __init__(self, db_path: str):
        self.db_path = db_path
        self.connection: Optional[aiosqlite.Connection] = None
        self.all_schemas: Optional[Dict[str, Dict[str, Any]]] = None

    async def __aenter__(self):
        """Async context manager entry."""
        self.connection = await aiosqlite.connect(self.db_path)
        # Load all schemas when entering the context
        self.all_schemas = await self.get_all_schemas()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        if self.connection:
            await self.connection.close()

    async def fetch_distinct_values(self, column: str, table: str) -> List[str]:
        cursor = await self.connection.execute(
            f"SELECT DISTINCT {column} FROM {table} WHERE {column} IS NOT NULL ORDER BY {column}"
        )
        return [row[0] for row in await cursor.fetchall() if row[0]]

    def infer_relationship_type(self, references_table: str) -> str:
        return (
            "many_to_one"
            if references_table in {"customers", "products"}
            else "one_to_many"
        )

    async def get_table_schema(self, table_name: str) -> Dict[str, Any]:
        """
        Get table schema in AI-friendly format for dynamic query generation.

        Args:
            table_name: Name of the table to analyze

        Returns:
            Dictionary with table metadata optimized for AI model consumption
        """
        if not self.connection:
            return {"error": "Database connection not established"}

        # Check if table exists
        cursor = await self.connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name = ?",
            (table_name,),
        )
        if not await cursor.fetchone():
            return {"error": f"Table '{table_name}' not found"}

        # Get table schema and foreign keys
        table_info_task = self.connection.execute(f"PRAGMA table_info({table_name})")
        fk_task = self.connection.execute(f"PRAGMA foreign_key_list({table_name})")
        columns, foreign_keys = await asyncio.gather(table_info_task, fk_task)
        columns = await columns.fetchall()
        foreign_keys = await foreign_keys.fetchall()

        columns_format = ", ".join(f"{col[1]}:{col[2]}" for col in columns)
        lower_table = table_name.lower()

        available_regions = None
        available_product_types = None
        available_product_categories = None
        reporting_years = None

        enum_queries = {
            "customers": {"available_regions": ("region", "customers")},
            "products": {
                "available_product_types": ("product_type", "products"),
                "available_product_categories": ("main_category", "products"),
            },
        }

        if lower_table in enum_queries:
            for key, (column, table) in enum_queries[lower_table].items():
                value = await self.fetch_distinct_values(column, table)
                if key == "available_regions":
                    available_regions = value
                elif key == "available_product_types":
                    available_product_types = value
                elif key == "available_product_categories":
                    available_product_categories = value

        if lower_table == "orders":
            cursor = await self.connection.execute(
                "SELECT DISTINCT strftime('%Y', order_date) as year FROM orders WHERE order_date IS NOT NULL ORDER BY year"
            )
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
        if available_regions:
            schema_data["available_regions"] = available_regions
        if available_product_types:
            schema_data["available_product_types"] = available_product_types
        if available_product_categories:
            schema_data["available_product_categories"] = available_product_categories
        if reporting_years:
            schema_data["reporting_years"] = reporting_years
        # Debug: print reporting_years for orders
        if lower_table == "orders":
            print(f"[DEBUG] reporting_years for orders: {reporting_years}")
        return schema_data

    async def get_all_table_names(self) -> List[str]:
        """Get list of all user table names."""
        if not self.connection:
            return []
        cursor = await self.connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
        )
        return [table[0] for table in await cursor.fetchall()]

    async def get_all_schemas(self) -> Dict[str, Dict[str, Any]]:
        """Get all table schemas in AI-friendly format."""
        table_names = await self.get_all_table_names()
        schemas = {}
        for table_name in table_names:
            schemas[table_name] = await self.get_table_schema(table_name)
        return schemas

    def _format_enum_field(self, label: str, values: Union[List[str], str]) -> str:
        if isinstance(values, list):
            if len(values) > 10:
                return f"{label}: {', '.join(values[:10])}, ... [{len(values)} total options]"
            return f"{label}: {', '.join(values)}"
        return f"{label}: {values}"

    def format_schema_metadata_for_ai(self, schema: Dict[str, Any]) -> str:
        """
        Convert a schema dictionary to an AI-optimized metadata string with markdown formatting.
        This format is specifically designed for AI model consumption with clear structure and context.

        Args:
            schema: Schema dictionary from get_all_schemas() or get_table_schema()

        Returns:
            AI-optimized formatted markdown string containing all schema metadata
        """
        if "error" in schema:
            return f"**ERROR:** {schema['error']}"

        lines = []
        table_name = schema.get("table_name", "Unknown")

        # Header with clear table identification using markdown heading
        lines.append(f"# Table: {table_name}")
        lines.append("")

        # Purpose section
        lines.append(
            f"**Purpose:** {schema.get('description', 'No description available')}"
        )
        lines.append("")

        # Schema section with markdown heading
        lines.append("## Schema")
        lines.append(f"{schema.get('columns_format', 'N/A')}")
        lines.append("")

        # Foreign key relationships with markdown heading
        if "foreign_keys" in schema and schema["foreign_keys"]:
            lines.append("## Relationships")
            for fk in schema["foreign_keys"]:
                relationship = fk.get("relationship_type", "unknown").upper()
                lines.append(
                    f"- `{fk['column']}` → `{fk['references_table']}.{fk['references_column']}` ({relationship})"
                )
            lines.append("")

        # Enumerated values section with markdown heading
        enum_fields = [
            ("available_regions", "Valid Regions"),
            ("available_product_types", "Valid Product Types"),
            ("available_product_categories", "Valid Categories"),
            ("reporting_years", "Available Years"),
        ]

        enum_sections_added = False
        for field_key, field_label in enum_fields:
            if field_key in schema and schema[field_key]:
                if not enum_sections_added:
                    lines.append("## Valid Values")
                    enum_sections_added = True

                values = schema[field_key]
                if isinstance(values, list) and len(values) > 10:
                    lines.append(
                        f"**{field_label}:** {', '.join(str(v) for v in values[:10])}, ... [{len(values)} total options]"
                    )
                else:
                    lines.append(
                        f"**{field_label}:** {', '.join(str(v) for v in values) if isinstance(values, list) else values}"
                    )

        if enum_sections_added:
            lines.append("")

        # Query hints section with markdown heading
        lines.append("## Query Hints")
        lines.append(
            f"- Use `{table_name}` for queries about {table_name.replace('_', ' ')}"
        )
        if "foreign_keys" in schema and schema["foreign_keys"]:
            for fk in schema["foreign_keys"]:
                lines.append(
                    f"- Join with `{fk['references_table']}` using `{fk['column']}`"
                )

        return "\n".join(lines) + "\n"

    async def get_table_metadata_string(self, table_name: str) -> str:
        """
        Get AI-formatted metadata string for a specific table by name.

        Args:
            table_name: Name of the table to get metadata for

        Returns:
            AI-optimized formatted metadata string for the table
        """
        if self.all_schemas and table_name in self.all_schemas:
            schema = self.all_schemas[table_name]
            return self.format_schema_metadata_for_ai(schema)
        else:
            # Fallback: get schema directly if not in preloaded schemas
            schema = await self.get_table_schema(table_name)
            return self.format_schema_metadata_for_ai(schema)

    async def execute_query(self, sqlite_query: str) -> str:
        """
        Execute SQLite queries against the database and return results as JSON.

        :param sqlite_query: The input should be a well-formed SQLite query to extract information based on the user's question.
                            The query result will be returned as a JSON object.
        :return: Return data in JSON serializable format.
        :rtype: str
        """
        if not self.connection:
            return json.dumps({"error": "Database connection not established"})

        print(f"\n🔍 Executing SQLite query: {sqlite_query}\n")

        try:
            # Perform the query asynchronously
            async with self.connection.execute(sqlite_query) as cursor:
                rows = await cursor.fetchall()
                columns = [description[0] for description in cursor.description]

            if not rows:  # No need to create DataFrame if there are no rows
                return json.dumps(
                    "The query returned no results. Try a different question."
                )

            # Convert to pandas DataFrame and return as JSON
            data = pd.DataFrame(rows, columns=columns)
            return data.to_json(index=False, orient="split")

        except Exception as e:
            return json.dumps(
                {"SQLite query failed with error": str(e), "query": sqlite_query}
            )


async def main():
    """Main function to demonstrate AI-friendly schema methods."""
    # Database path
    db_path = Path(__file__).parent / "shared" / "database" / "customer_sales.db"

    if not db_path.exists():
        print(f"❌ Error: Database file not found at {db_path}")
        print("   Please run the database generator first:")
        print("   python shared/database/data-generator/generate_customer_db.py")
        return

    print("🤖 AI-Friendly Database Schema Tool")
    print("=" * 50)

    try:
        async with DatabaseSchemaProvider(str(db_path)) as provider:
            # Get all schemas (now pre-loaded in __aenter__)
            print("\n📋 Getting all table schemas...")
            all_schemas = provider.all_schemas

            if not all_schemas:
                print("❌ No schemas available")
                return

            # Test SQL query execution
            print("\n🧪 Testing SQL Query Execution:")
            print("=" * 50)

            # Test 1: Simple count query
            print("\n📊 Test 1: Count all customers")
            result = await provider.execute_query(
                "SELECT COUNT(*) as total_customers FROM customers"
            )
            print(f"Result: {result}")

            print("\n✅ SQL Query tests completed!")

            print("=" * 50)
            print(await provider.get_table_metadata_string("customers"))
            print("=" * 50)
            print(await provider.get_table_metadata_string("products"))
            print("=" * 50)
            print(await provider.get_table_metadata_string("orders"))

    except Exception as e:
        print(f"❌ Error during analysis: {e}")
        raise


if __name__ == "__main__":
    # Run the schema analysis
    asyncio.run(main())
