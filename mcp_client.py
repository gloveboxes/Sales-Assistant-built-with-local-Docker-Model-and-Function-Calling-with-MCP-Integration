"""
MCP Client integration for the docker model runner.
Provides tools to communicate with MCP servers.
"""

import asyncio
import sys
from typing import Any, Dict, List, Optional

from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client


class MCPClient:
    """Client for communicating with MCP servers with optimized connection handling."""

    def __init__(self, server_command: List[str]):
        """Initialize with the command to start the MCP server."""
        self.server_command = server_command
        self.server_params = StdioServerParameters(
            command=self.server_command[0],
            args=self.server_command[1:] if len(self.server_command) > 1 else [],
        )

    async def call_tool_async(self, tool_name: str, arguments: Dict[str, Any]) -> str:
        """Call a tool on the MCP server using a fresh connection."""
        try:
            # server_params = StdioServerParameters(
            #     command=self.server_command[0],
            #     args=self.server_command[1:] if len(self.server_command) > 1 else [],
            # )

            async with stdio_client(self.server_params) as (read, write):
                async with ClientSession(read, write) as session:
                    await session.initialize()
                    result = await session.call_tool(tool_name, arguments)

                    if result.content and len(result.content) > 0:
                        content_item = result.content[0]
                        if hasattr(content_item, "text"):
                            return getattr(content_item, "text")
                        else:
                            return str(content_item)
                    else:
                        return "No result returned from tool"

        except Exception as e:
            return f"Error calling tool {tool_name}: {e}"

    async def fetch_tools_async(self) -> List[Dict]:
        """Fetch tool schemas from MCP server in OpenAI function format."""
        try:
            # server_params = StdioServerParameters(
            #     command=self.server_command[0],
            #     args=self.server_command[1:] if len(self.server_command) > 1 else [],
            # )

            async with stdio_client(self.server_params) as (read, write):
                async with ClientSession(read, write) as session:
                    await session.initialize()
                    tools_result = await session.list_tools()

                    # Convert MCP Tool objects to OpenAI function format
                    openai_tools = []
                    for tool in tools_result.tools:
                        openai_tool = {
                            "type": "function",
                            "function": {
                                "name": tool.name,
                                "description": tool.description,
                                "parameters": tool.inputSchema,
                            },
                        }
                        openai_tools.append(openai_tool)

                    return openai_tools

        except Exception:
            print("Error fetching tools from MCP server")
            # Fallback to static tools if dynamic fetch fails
            return []


# Global MCP client instance for reuse
_mcp_client: Optional[MCPClient] = None


def get_mcp_client() -> MCPClient:
    """Get or create the MCP client instance."""
    global _mcp_client
    if _mcp_client is None:
        server_command = [sys.executable, "mcp_server.py"]
        _mcp_client = MCPClient(server_command)
    return _mcp_client


async def cleanup_global_mcp_client():
    """Cleanup the global MCP client instance."""
    global _mcp_client
    # With the simplified approach, no cleanup is needed
    _mcp_client = None


# Utility functions for integration
async def call_mcp_tool_async(tool_name: str, **kwargs) -> str:
    """Call an MCP tool with the given arguments."""
    client = get_mcp_client()
    return await client.call_tool_async(tool_name, kwargs)


async def fetch_mcp_tools_async() -> List[Dict]:
    """Fetch tool schemas from MCP server in OpenAI function format."""
    client = get_mcp_client()
    return await client.fetch_tools_async()


# Synchronous wrapper functions for use in the main app
def initialize_mcp_sync():
    """Synchronous wrapper for MCP initialization."""
    try:
        # Just test if we can create a simple client
        return True  # Simplified for now
    except Exception:
        print("Error initializing MCP client")
        return False


if __name__ == "__main__":
    # Simple test
    async def test():
        tools = await fetch_mcp_tools_async()
        print(f"Available tools: {[tool['function']['name'] for tool in tools]}")

        if tools:
            tool_name = tools[0]["function"]["name"]
            result = await call_mcp_tool_async(tool_name)
            print(f"Test result: {result}")

    asyncio.run(test())
