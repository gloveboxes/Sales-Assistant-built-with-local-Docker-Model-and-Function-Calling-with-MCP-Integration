# AI Assistant with Function Calling and MCP Integration

A modular, async-friendly AI chatbot that demonstrates the integration of Model Context Protocol (MCP) tools with local AI models. This project showcases how to build a sales analysis agent for Contoso (a fictional outdoor gear retailer) using function calling capabilities with a locally hosted AI model.

## 📊 Scenario

Imagine you are a sales manager at Contoso, a multinational retail company that sells outdoor equipment. You need to analyze sales data to find trends, understand customer preferences, and make informed business decisions. To help you, Contoso has developed a conversational agent that can answer questions about your sales data.

This project demonstrates how such an agent works behind the scenes, combining the power of local AI models with database tools through the Model Context Protocol.

## 🚀 Features

- **Local AI Model**: Uses the `ai/phi4:14B-Q4_0` model hosted with Docker Model Runner
- **MCP Integration**: Implements Model Context Protocol for tool communication
- **Database Analysis**: SQLite database queries for sales data analysis
- **Function Calling**: Advanced AI function calling capabilities
- **Async Architecture**: Fully asynchronous implementation for better performance
- **Sales Analytics**: Specialized tools for analyzing Contoso sales data

## 🏗️ Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   AI Assistant  │    │   MCP Client    │    │   MCP Server    │
│   (main.py)     │◄──►│ (mcp_client.py) │◄──►│ (mcp_server.py) │
│                 │    └─────────────────┘    └─────────────────┘
│ ┌─────────────┐ │                                     │
│ │ OpenAI      │ │                                     ▼
│ │ Client      │ │                            ┌─────────────────┐
│ │ Interface   │ │                            │   Sales Data    │
│ └─────────────┘ │                            │ (sales_data.py) │
└─────────────────┘                            └─────────────────┘
         │                                               │
         ▼                                               ▼
┌─────────────────┐                            ┌─────────────────┐
│ Docker Model    │                            │ SQLite Database │
│ Runner (Local)  │                            │ (contoso-sales) │
└─────────────────┘                            └─────────────────┘
```

## 📋 Prerequisites

- **Python 3.8+**
- **Docker Desktop 4.42+** with Docker Model Runner enabled
- **System Requirements**: The Phi4 14B model requires significant resources:
  - Recommended: 16GB+ RAM
  - GPU acceleration supported on Apple Silicon (macOS) and NVIDIA GPUs (Windows)
- **Docker Model Runner**: See setup instructions below

## 🛠️ Installation

1. **Clone the repository**:

   ```bash
   git clone <repository-url>
   cd function_calling_and_mcp
   ```

2. **Create and activate a virtual environment**:

   **Linux/macOS:**

   ```bash
   python3 -m venv venv
   source venv/bin/activate
   ```

   **Windows:**

   ```cmd
   python -m venv venv
   venv\Scripts\activate
   ```

3. **Install Python dependencies**:

   ```bash
   pip install -r requirements.txt
   ```

4. **Set up Docker Model Runner**:

   **Enable Docker Model Runner** ([full documentation](https://docs.docker.com/ai/model-runner/)):
   - **Docker Desktop**: Go to Settings → Beta features → Enable "Docker Model Runner"
   - **Docker Engine**: Install the plugin with `sudo apt-get install docker-model-plugin` (Ubuntu/Debian)

   **Pull and run the Phi4 model**:

   ```bash
   # Pull the model (first time only)
   docker model pull ai/phi4:14B-Q4_0
   ```

   The model will be available at `http://localhost:12434` with OpenAI-compatible APIs.

   > **Note**: The [Phi4 model](https://hub.docker.com/r/ai/phi4) is a 14B parameter model optimized for reasoning and requires significant system resources.

5. **Database Setup**:
   The SQLite database (`shared/database/contoso-sales.db`) contains sample sales data for Contoso. No additional setup is required as it's included in the repository.

## 🚀 Usage

1. **Start the AI Assistant**:

   ```bash
   python main.py
   ```

2. **Interact with the Assistant**:

   ```shell
   🤖 AI Assistant Ready!
   Type 'exit' to quit.
   Available tools: get_database_schema, fetch_sales_data_using_sqlite_query
   =========================================================================
   
   You: What were the total sales by region in 2022?
   ```

3. **Example Queries**:
   - "help"
   - "Show me total revenue by product category"
   - "What were the top performing regions in 2023?"

## 📁 Project Structure

```text
function_calling_and_mcp/
├── main.py                 # Main AI assistant application
├── mcp_client.py          # MCP client for tool communication
├── mcp_server.py          # MCP server with database tools
├── sales_data.py          # Database access layer
├── utilities.py           # Utility functions
├── terminal_colors.py     # Terminal color formatting
├── system_msg.txt         # System prompt for the AI
├── requirements.txt       # Python dependencies
└── shared/
    └── database/
        ├── contoso-sales.db          # SQLite database
        ├── sales_data.sqbpro         # Database project file
        └── data-generator/
            ├── generate_sql.py       # Data generation script
            └── populate_sales_data.sql # SQL for populating data
```

## 🧩 Key Components

### AIAssistant (main.py)

The main orchestrator that:

- Manages conversation flow
- Coordinates with MCP tools
- Handles function calling
- Integrates with the local AI model

### MCPClient (mcp_client.py)

Handles communication with MCP servers:

- Establishes connections to MCP servers
- Executes tool calls
- Manages tool schemas

### MCPServer (mcp_server.py)

Provides tools for database access:

- `get_database_schema`: Returns database structure information
- `fetch_sales_data_using_sqlite_query`: Executes SQL queries on sales data

### SalesData (sales_data.py)

Database access layer providing:

- Async SQLite connections
- Schema introspection
- Sales data querying
- Data validation and formatting

## 🔧 Configuration

### Model Configuration

Edit the `ModelConfig` in `main.py`:

```python
@dataclass
class ModelConfig:
    base_url: str = "http://localhost:12434/engines/llama.cpp/v1"
    api_key: str = "docker"
    model_name: str = "ai/phi4:14B-Q4_0"
    max_tokens: int = 4096
```

### Database Configuration

The database path is configured in `sales_data.py`:

```python
DATA_BASE = "database/contoso-sales.db"
```

## 📊 Database Schema

The Contoso sales database contains a single table with comprehensive sales transaction data:

### Table: `sales_data`

**Columns:**

- `id`: INTEGER (Primary key)
- `main_category`: TEXT (Product category)
- `product_type`: TEXT (Specific product type)
- `revenue`: REAL (Sales revenue in dollars)
- `shipping_cost`: REAL (Shipping cost in dollars)
- `number_of_orders`: INTEGER (Number of orders)
- `year`: INTEGER (Year of transaction)
- `month`: INTEGER (Month number 1-12)
- `discount`: INTEGER (Discount percentage)
- `region`: TEXT (Geographic region)
- `month_date`: TEXT (Month name)

### Available Data Values

**Regions (7):**
AFRICA, ASIA-PACIFIC, CHINA, EUROPE, LATIN AMERICA, MIDDLE EAST, NORTH AMERICA

**Product Categories (8):**
APPAREL, CAMPING & HIKING, CLIMBING, FISHING GEAR, FOOTWEAR, TRAVEL, WATER GEAR, WINTER SPORTS

**Product Types (80+):**
ACCESSORIES, AVALANCHE SAFETY, BACKPACKING TENTS, BINDINGS, BIVYS, BOULDERING PADS, CANOES, CARABINERS & QUICKDRAWS, CARRY-ONS, CHALK & CHALK BAGS, CLIMBING SHOES, COOKWARE, CRAMPONS, DAYPACKS, DRY BAGS, DUFFEL BAGS, EXTENDED TRIP PACKS, EYE MASKS, FAMILY CAMPING TENTS, FIRST AID KITS, FISHING BAIT, FISHING HOOKS, FISHING LINE, FOOD & NUTRITION, FOOTWEAR ACCESSORIES, FOOTWEAR CARE PRODUCTS, GLOVES & HATS, GLOVES & MITTENS, GOGGLES, HAMMOCKS, HARNESSES, HELMETS, HIKING BOOTS, HYDRATION PACKS, ICE AXES, INSULATED FOOTWEAR, JACKETS & VESTS, KAYAKS, LINERS, LUGGAGE LOCKS, MOUNTAINEERING BOOTS, NAVIGATION TOOLS, OUTERWEAR, OVERNIGHT PACKS, PACKING ORGANIZERS, PADDLES, PANTS & SHORTS, POLES, RASH GUARDS, RODS & REELS, ROPES & SLINGS, SAFETY GEAR, SANDALS, SHELTERS & TARPS, SHIRTS, SKI BINDINGS, SKI BOOTS, SKI POLES, SKIS, SLACKLINES, SLEEPING BAGS, SLEEPING PADS, SNORKELING & DIVING GEAR, SNOWBOARD BOOTS, SNOWBOARDS, SNOWSHOES, STOVES, SURF ACCESSORIES, SURFBOARDS, SWIMWEAR, TACKLE, TECH ORGANIZERS, THERMAL UNDERWEAR, TOPS, TRAIL SHOES, TRAINING EQUIPMENT, TRAVEL ACCESSORIES, TRAVEL BACKPACKS, TRAVEL PILLOWS, UNDERWEAR & BASE LAYERS, UTENSILS & ACCESSORIES, WADERS, WATER FILTRATION & PURIFICATION, WETSUITS, WINTER BOOTS

**Years:** 2022, 2023, 2024

**Months:** 1-12 (January through December)

## 🎯 Use Cases

1. **Sales Analysis**: Query and analyze sales performance across different dimensions
2. **Business Intelligence**: Generate insights from sales data
3. **Reporting**: Create automated reports for stakeholders
4. **Trend Analysis**: Identify sales patterns and trends
5. **MCP Development**: Learn how to integrate MCP tools with AI assistants

## 🛡️ Error Handling

The application includes comprehensive error handling:

- Database connection failures
- MCP server communication errors
- Invalid SQL queries
- Model response errors
- Graceful degradation when tools are unavailable

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## 📝 License

This project is provided as an example implementation for educational purposes.

## 🆘 Troubleshooting

### Common Issues

1. **Docker Model Runner Connection**:
   - Ensure Docker Model Runner is running on port 12434
   - Verify the model `ai/phi4:14B-Q4_0` is loaded

1. **Function Calling Issues**:
   - Verify the AI model supports function calling
   - Check tool schema formatting

### Getting Help

If you encounter issues:

1. Check the terminal output for error messages
2. Verify all dependencies are installed
3. Ensure the Docker Model Runner is properly configured
4. Review the logs for debugging information

---

**Note**: This is a demonstration project showcasing MCP integration with local AI models. The Contoso company and sales data are fictional and used for educational purposes.
