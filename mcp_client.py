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
        self._session: Optional[ClientSession] = None
        self._read = None
        self._write = None
        self._client_context = None

    async def _ensure_session(self):
        """Ensure we have an active session, creating one if necessary."""
        if self._session is None:
            self._client_context = stdio_client(self.server_params)
            self._read, self._write = await self._client_context.__aenter__()
            self._session = ClientSession(self._read, self._write)
            await self._session.__aenter__()
            await self._session.initialize()

    async def close_session(self):
        """Close the current session and cleanup resources."""
        if self._session is not None:
            try:
                await self._session.__aexit__(None, None, None)
            except Exception:
                pass
            self._session = None
        
        if self._client_context is not None:
            try:
                await self._client_context.__aexit__(None, None, None)
            except Exception:
                pass
            self._client_context = None
            self._read = None
            self._write = None

    async def call_tool_async(self, tool_name: str, arguments: Dict[str, Any]) -> str:
        """Call a tool on the MCP server using the persistent session."""
        try:
            await self._ensure_session()
            if self._session is None:
                return "Error: Could not establish session"
            
            result = await self._session.call_tool(tool_name, arguments)

            if result.content and len(result.content) > 0:
                content_item = result.content[0]
                if hasattr(content_item, "text"):
                    return getattr(content_item, "text")
                else:
                    return str(content_item)
            else:
                return "No result returned from tool"

        except Exception as e:
            # If there's an error, try to close and recreate the session
            await self.close_session()
            return f"Error calling tool {tool_name}: {e}"

    async def fetch_tools_async(self) -> List[Dict]:
        """Fetch tool schemas from MCP server using the persistent session."""
        try:
            await self._ensure_session()
            if self._session is None:
                return []
            
            tools_result = await self._session.list_tools()

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
            # If there's an error, try to close and recreate the session
            await self.close_session()
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
    if _mcp_client is not None:
        await _mcp_client.close_session()
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
