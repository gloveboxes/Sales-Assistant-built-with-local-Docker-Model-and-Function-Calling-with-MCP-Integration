"""
AI Assistant with Function Calling Support

A modular, async-friendly chatbot that integrates with MCP (Model Context Protocol)
tools for enhanced functionality including database queries and analysis.

Requirements:
    pip install openai mcp

Usage:
    python docker_model_runner_function_calling.py
"""

import asyncio
import json
import sys
from dataclasses import dataclass
from typing import Dict, List, Optional, Any

from openai import OpenAI
from mcp_client import sync_initialize_mcp, call_mcp_tool_async, fetch_mcp_tools_async
from terminal_colors import TerminalColors as tc


@dataclass
class ModelConfig:
    """Configuration for the AI model."""

    base_url: str = "http://localhost:12434/engines/llama.cpp/v1"
    api_key: str = "docker"
    model_name: str = "ai/phi4:14B-Q4_0"
    max_tokens: int = 4096


class MCPToolManager:
    """Manages MCP tools and their execution."""

    def __init__(self):
        self.tools: List[Dict] = []
        self.available_tools: set = set()
        self.is_initialized = False

    async def initialize(self) -> bool:
        """Initialize MCP server and load available tools."""
        print("🔧 Initializing MCP server...")

        # Initialize MCP connection
        mcp_success = sync_initialize_mcp()
        if not mcp_success:
            print(
                "❌ Failed to connect to MCP server - database tools will not be available"
            )
            return False

        print("✅ MCP server connected successfully")

        # Fetch available tools
        print("📡 Fetching available tools from MCP server...")
        try:
            self.tools = await fetch_mcp_tools_async()

            if not self.tools:
                print("⚠️  No tools found from MCP server")
                return False

            # Create function mappings
            self._create_function_mappings()

            available_tool_names = [tool["function"]["name"] for tool in self.tools]
            print(f"✅ Available MCP tools: {', '.join(available_tool_names)}")

            self.is_initialized = True
            return True

        except Exception as e:
            print(f"❌ Error fetching tools: {e}")
            return False

    def _create_function_mappings(self):
        """Create dynamic function mappings for all available tools."""
        self.available_tools = set()
        for tool in self.tools:
            tool_name = tool["function"]["name"]
            self.available_tools.add(tool_name)

    async def execute_tool(self, tool_name: str, arguments: Dict[str, Any]) -> str:
        """Execute a tool with given arguments."""
        if tool_name not in self.available_tools:
            return f"Unknown function: {tool_name}"

        # Special handling for database queries
        if tool_name == "fetch_sales_data_using_sqlite_query":
            query = arguments.get("sqlite_query", "No query provided")
            print(f"{tc.BG_BLUE}Executing query: {query}{tc.RESET}")

        try:
            result = await call_mcp_tool_async(tool_name, **arguments)
            return result
        except Exception as e:
            return f"Error executing {tool_name}: {e}"


class ConversationManager:
    """Manages conversation history and message flow."""

    def __init__(self):
        self.history: List[Dict[str, Any]] = []
        self._add_system_message()

    def _add_system_message(self):
        """Add initial system message with Contoso Sales Analysis Agent instructions."""
        try:
            # Read system message from file
            with open("system_msg.txt", "r", encoding="utf-8") as f:
                content = f.read().strip()

            system_message = {"role": "system", "content": content}
            self.history.append(system_message)
        except FileNotFoundError:
            print(
                "⚠️  Warning: system_msg.txt not found - no system message will be added"
            )
        except Exception as e:
            print(f"⚠️  Warning: Could not read system_msg.txt: {e}")

    def add_user_message(self, content: str):
        """Add a user message to the conversation history."""
        self.history.append({"role": "user", "content": content})

    def add_assistant_message(
        self, content: str, tool_calls: Optional[List[Any]] = None
    ):
        """Add an assistant message to the conversation history."""
        message: Dict[str, Any] = {"role": "assistant", "content": content}
        if tool_calls:
            message["tool_calls"] = tool_calls  # type: ignore
        self.history.append(message)

    def add_tool_result(self, tool_call_id: str, content: str):
        """Add a tool execution result to the conversation history."""
        self.history.append(
            {
                "role": "tool",
                "tool_call_id": tool_call_id,
                "content": content,
            }
        )

    def get_history(self) -> List[Dict[str, Any]]:
        """Get the current conversation history."""
        return self.history


class AIAssistant:
    """Main AI Assistant class that coordinates all components."""

    def __init__(self, config: ModelConfig):
        self.config = config
        self.client = OpenAI(
            base_url=config.base_url,
            api_key=config.api_key,
        )
        self.tool_manager = MCPToolManager()
        self.conversation = ConversationManager()

    async def initialize(self) -> bool:
        """Initialize the assistant and its components."""
        success = await self.tool_manager.initialize()
        if success:
            self._print_welcome_message()
        return success

    def _print_welcome_message(self):
        """Print welcome message and available functions."""
        print("\n" + "=" * 60)
        print("🤖 AI Assistant Ready!")
        print("Type 'exit' to quit.")

        if self.tool_manager.is_initialized:
            tool_names = [tool["function"]["name"] for tool in self.tool_manager.tools]
            print(f"📋 Available functions: {', '.join(tool_names)}")
            print("💡 Ask about Contoso sales data!")

        print("=" * 60)

    async def chat(self):
        """Main chat loop."""
        while True:
            try:
                # Get user input
                user_input = input("\n🧑 You: ").strip()

                if not user_input:
                    print("Please enter a message or type 'exit' to quit.")
                    continue

                if user_input.lower() == "exit":
                    print("👋 Goodbye!")
                    break

                # Process the message
                await self._process_message(user_input)

            except KeyboardInterrupt:
                print("\n\n👋 Goodbye!")
                break
            except Exception as e:
                print(f"❌ Error: {e}")
                print("Please try again or type 'exit' to quit.")

    async def _process_message(self, user_input: str):
        """Process a user message and generate response."""
        # Add user message to history
        self.conversation.add_user_message(user_input)

        try:
            # Get initial response from AI
            response = self._get_ai_response()

            # Handle function calls if present
            while response.choices[0].message.tool_calls:
                await self._handle_function_calls(response)
                # Get follow-up response
                response = self._get_ai_response()

            # Handle final response (no function calls)
            content = response.choices[0].message.content
            print(f"\n🤖 Assistant: {content}")
            self.conversation.add_assistant_message(content)

        except Exception as e:
            print(f"❌ Error processing message: {e}")

    def _get_ai_response(self):
        """Get response from the AI model."""
        # Convert messages to proper format for OpenAI API
        messages = self.conversation.get_history()
        tools = self.tool_manager.tools if self.tool_manager.tools else None

        return self.client.chat.completions.create(
            messages=messages,  # type: ignore
            model=self.config.model_name,
            max_tokens=self.config.max_tokens,
            tools=tools,  # type: ignore
            tool_choice="auto",
            stream=False,
        )

    async def _handle_function_calls(self, response):
        """Handle function calls in the AI response."""
        message = response.choices[0].message

        if message.content:
            print(f"\n🤖 Assistant: {message.content}")

        # Add assistant message with tool calls to history
        self.conversation.add_assistant_message(message.content, message.tool_calls)

        # Process each tool call
        for tool_call in message.tool_calls:
            if tool_call.function and tool_call.function.name:
                await self._execute_tool_call(tool_call)

    async def _execute_tool_call(self, tool_call):
        """Execute a single tool call."""
        function_name = tool_call.function.name
        print(f"\n🔧 [Function call: {function_name}]")

        try:
            # Parse arguments
            args = json.loads(tool_call.function.arguments)

            # Execute the tool
            result = await self.tool_manager.execute_tool(function_name, args)

            print(f"📊 Result: {result}")

            # Add result to conversation history
            self.conversation.add_tool_result(tool_call.id, result)

        except json.JSONDecodeError as e:
            error_msg = f"Invalid JSON arguments for {function_name}: {e}"
            print(f"❌ {error_msg}")
            self.conversation.add_tool_result(tool_call.id, error_msg)
        except Exception as e:
            error_msg = f"Error executing {function_name}: {e}"
            print(f"❌ {error_msg}")
            self.conversation.add_tool_result(tool_call.id, error_msg)


async def main():
    """Main entry point."""
    try:
        # Create and initialize the assistant
        config = ModelConfig()
        assistant = AIAssistant(config)

        # Initialize components
        if not await assistant.initialize():
            print("❌ Failed to initialize assistant")
            sys.exit(1)

        # Start chat loop
        await assistant.chat()

    except Exception as e:
        print(f"❌ Fatal error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
