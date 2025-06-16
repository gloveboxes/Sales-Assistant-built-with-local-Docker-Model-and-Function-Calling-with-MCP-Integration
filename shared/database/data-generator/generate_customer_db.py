import random
import datetime
import sqlite3
import os
from faker import Faker

fake = Faker()

main_categories = {  
    "APPAREL": {  
        "JACKETS & VESTS": ["Summit Pro Insulated Jacket", 50, 250, "High-performance jackets and vests designed for outdoor adventures, featuring weather-resistant materials and insulation for all-season comfort"],  
        "RAIN JACKETS": ["StormShield Waterproof Jacket", 45, 180, "Lightweight waterproof jackets with breathable membranes for wet weather protection"],
        "FLEECE JACKETS": ["CozyWarm Fleece Jacket", 35, 120, "Soft fleece jackets for mid-layer warmth and comfort in cool conditions"],
        "SOFTSHELL JACKETS": ["FlexGuard Softshell Jacket", 60, 200, "Stretchy softshell jackets with wind and water resistance for active pursuits"],
        "OTHER": ["Trail Accessories Bundle", 10, 100, "Miscellaneous apparel items including accessories, belts, and specialty outdoor clothing"],  
        "PANTS & SHORTS": ["Adventure Flex Hiking Pants", 30, 120, "Durable outdoor pants and shorts with moisture-wicking fabrics, reinforced knees, and articulated fit for active pursuits"],  
        "CARGO PANTS": ["Explorer Cargo Pants", 40, 130, "Multi-pocket cargo pants with reinforced construction for rugged outdoor use"],
        "SHORTS": ["QuickDry Trail Shorts", 25, 90, "Lightweight trail shorts with moisture-wicking fabric and secure pockets"],
        "SHIRTS": ["Alpine Tech Performance Shirt", 20, 80, "Technical shirts and casual outdoor wear with UV protection, breathable fabrics, and quick-dry technology"],  
        "POLO SHIRTS": ["Outdoor Explorer Polo", 30, 70, "Casual polo shirts with UV protection and moisture management for outdoor lifestyle"],
        "TOPS": ["Mountain Thermal Fleece", 15, 60, "Performance tops including fleece, sweaters, and layering pieces for temperature regulation during outdoor activities"],  
        "UNDERWEAR & BASE LAYERS": ["Merino Base Layer Set", 10, 50, "Moisture-wicking underwear and base layers designed for thermal regulation and comfort during extended outdoor activities"],
        "SOCKS": ["Performance Trail Socks", 8, 25, "Cushioned hiking socks with moisture management and blister prevention"],
        "HATS & CAPS": ["Adventure Sun Hat", 15, 45, "Wide-brim sun hats and caps for UV protection during outdoor activities"],
        "GLOVES": ["Multi-Sport Outdoor Gloves", 12, 60, "Versatile outdoor gloves with grip and dexterity for various activities"],
        "BELTS": ["Adventure Utility Belt", 18, 50, "Durable outdoor belts with quick-release buckles and utility features"]  
    },  
    "CAMPING & HIKING": {  
        "BACKPACKING TENTS": ["Ultralight Summit 2P Tent", 100, 500, "Lightweight, compact tents designed for multi-day backpacking trips with superior weather protection and easy setup"],  
        "BIVYS": ["Alpine Emergency Shelter", 50, 200, "Ultra-lightweight emergency shelters and bivy sacks for minimalist camping and emergency situations"],  
        "COOKWARE": ["Trail Chef Titanium Set", 20, 150, "Portable camping cookware including pots, pans, and cooking systems designed for outdoor meal preparation"],  
        "WATER BOTTLES": ["HydroTrail Insulated Bottle", 15, 60, "Insulated water bottles with leak-proof design for hydration on the trail"],
        "HEADLAMPS": ["NightVision Pro Headlamp", 25, 120, "Bright LED headlamps with multiple modes and long battery life for hands-free illumination"],
        "DAYPACKS": ["Explorer 25L Daypack", 30, 150, "Comfortable day hiking packs with organizational features and hydration compatibility for single-day adventures"],  
        "EXTENDED TRIP PACKS": ["Expedition 75L Backpack", 150, 400, "Large-capacity backpacks designed for multi-week expeditions with advanced suspension systems and durability"],  
        "FAMILY CAMPING TENTS": ["Family Base Camp 6P Tent", 200, 800, "Spacious family tents with multiple rooms, easy setup, and weather protection for car camping and group adventures"],  
        "CAMPING CHAIRS": ["ComfortBase Camp Chair", 40, 120, "Portable camping chairs with cup holders and sturdy construction for outdoor relaxation"],
        "FOOD & NUTRITION": ["Trail Energy Pack", 5, 50, "Outdoor nutrition including energy bars, dehydrated meals, and supplements for sustained energy during activities"],  
        "HAMMOCKS": ["Comfort Sleep Hammock System", 30, 150, "Portable hammocks and suspension systems for comfortable outdoor relaxation and lightweight camping"],  
        "HYDRATION PACKS": ["HydroFlow 3L System", 40, 120, "Hands-free hydration systems with reservoir bladders and ergonomic designs for active pursuits"],  
        "LINERS": ["ThermalComfort Sleeping Liner", 10, 60, "Sleeping bag liners and tent footprints for added comfort, warmth, and gear protection"],  
        "OTHER": ["Camp Essential Kit", 5, 100, "Miscellaneous camping gear including tools, repair kits, and specialty outdoor equipment"],  
        "OVERNIGHT PACKS": ["Weekend Adventure 45L Pack", 80, 250, "Mid-size backpacks perfect for overnight trips and weekend adventures with balanced capacity and comfort"],  
        "SHELTERS & TARPS": ["All-Weather Tarp Shelter", 40, 200, "Versatile tarps and shelters for weather protection, camp setup, and emergency situations"],  
        "SLEEPING BAGS": ["Four Season Sleep System", 60, 300, "Insulated sleeping bags rated for various temperatures with compressible designs for outdoor sleeping comfort"],  
        "SLEEPING PADS": ["ComfortPro Inflatable Pad", 30, 150, "Insulated and self-inflating sleeping pads for comfort and thermal protection while camping"],  
        "STOVES": ["Efficient Flame Stove", 20, 150, "Portable camping stoves and fuel systems for efficient outdoor cooking and meal preparation"],  
        "UTENSILS & ACCESSORIES": ["Camp Kitchen Essentials", 5, 50, "Camping utensils, plates, cups, and accessories for outdoor dining and food preparation"],
        "LANTERNS": ["BaseCamp LED Lantern", 18, 80, "Bright LED lanterns with multiple modes and long battery life for campsite illumination"]  
    },  
    "CLIMBING": {  
        "AVALANCHE SAFETY": ["Backcountry Safety Pro Kit", 100, 500, "Essential avalanche safety equipment including beacons, probes, and shovels for backcountry snow travel"],  
        "CARABINERS & QUICKDRAWS": ["Pro Climbing Hardware Set", 5, 50, "High-strength carabiners and quickdraws for rock climbing, mountaineering, and rope work applications"],  
        "CHALK & CHALK BAGS": ["Grip Master Chalk System", 5, 30, "Climbing chalk and chalk bags for improved grip and moisture management during climbing activities"],  
        "CLIMBING SHOES": ["Precision Climb Shoes", 60, 200, "Specialized climbing footwear with sticky rubber soles and precise fit for optimal performance on rock"],  
        "APPROACH SHOES": ["RockWalk Approach Shoes", 80, 180, "Versatile approach shoes with climbing rubber and hiking comfort for technical terrain"],
        "CRAMPONS": ["Ice Master Pro Crampons", 100, 300, "Steel and aluminum crampons for ice climbing and mountaineering with secure attachment systems"],  
        "HARNESSES": ["Safety First Climbing Harness", 50, 200, "Climbing harnesses with gear loops and belay devices for safety and comfort during vertical adventures"],  
        "HELMETS": ["Impact Shield Helmet", 40, 150, "Lightweight climbing helmets with impact protection for rock climbing, mountaineering, and ice climbing"],  
        "ICE AXES": ["Alpine Master Ice Axe", 50, 300, "Technical ice axes and ice tools for mountaineering, ice climbing, and alpine adventures"],  
        "MOUNTAINEERING BOOTS": ["Summit Pro Mountaineering Boots", 150, 500, "Insulated mountaineering boots with crampon compatibility for high-altitude and cold weather climbing"],  
        "BELAY DEVICES": ["SecureGrip Belay Device", 25, 80, "Advanced belay devices with assisted braking for enhanced climbing safety"],
        "OTHER": ["Climbing Essentials Pack", 10, 100, "Miscellaneous climbing gear including approach shoes, gloves, and specialty climbing accessories"],  
        "ROPES & SLINGS": ["Dynamic Safety Rope System", 30, 300, "Dynamic climbing ropes, slings, and cordage for safety systems and rigging in climbing applications"],  
        "TRAINING EQUIPMENT": ["Strength Builder Training Kit", 20, 150, "Climbing training tools including hangboards, resistance trainers, and technique development aids"]  
    },  
    "FOOTWEAR": {  
        "HIKING BOOTS": ["TrailMaster Hiking Boots", 60, 250, "Sturdy hiking boots with ankle support, waterproof membranes, and aggressive tread for trail hiking"],  
        "LIGHTWEIGHT BOOTS": ["FeatherStep Hiking Boots", 70, 220, "Lightweight hiking boots with modern materials for comfort and performance on long trails"],
        "OTHER": ["Outdoor Footwear Collection", 20, 100, "Miscellaneous outdoor footwear including casual shoes, slippers, and specialty athletic footwear"],  
        "SANDALS": ["Adventure Sport Sandals", 20, 80, "Outdoor sandals with supportive footbeds and durable straps for water sports and casual outdoor wear"],  
        "WATER SHOES": ["AquaGrip Water Shoes", 25, 70, "Quick-dry water shoes with drainage and grip for water activities and beach walking"],
        "TRAIL SHOES": ["Swift Trail Running Shoes", 50, 150, "Lightweight trail running shoes with grip and protection for fast-paced outdoor activities"],  
        "CASUAL SHOES": ["Urban Explorer Shoes", 40, 120, "Casual outdoor lifestyle shoes with comfort and style for everyday adventures"],
        "WINTER BOOTS": ["Arctic Pro Winter Boots", 60, 200, "Insulated winter boots with waterproof construction and traction for cold weather outdoor activities"]  
    },  
    "TRAVEL": {  
        "CARRY-ONS": ["Journey Pro Carry-On", 50, 200, "Airline-compliant carry-on luggage with organizational features and durable construction for frequent travel"],  
        "CHECKED LUGGAGE": ["Voyager Large Suitcase", 80, 300, "Spacious checked luggage with spinner wheels and TSA-approved locks for extended travel"],
        "DUFFEL BAGS": ["Adventure Duffel Collection", 30, 150, "Versatile duffel bags in various sizes for travel, gym use, and gear transport with multiple carry options"],  
        "ROLLING DUFFELS": ["Expedition Wheeled Duffel", 60, 220, "Large wheeled duffel bags for gear transport with rugged construction and smooth-rolling wheels"],
        "EYE MASKS": ["Rest Easy Sleep Mask", 5, 20, "Comfortable sleep masks for travel rest and light blocking during transportation and accommodation"],  
        "OTHER": ["Travel Essentials Kit", 5, 50, "Miscellaneous travel accessories including adapters, organizers, and convenience items"],  
        "PACKING ORGANIZERS": ["Smart Pack Organizer Set", 10, 50, "Packing cubes and organizers for efficient luggage organization and space maximization"],  
        "COMPRESSION SACKS": ["SpaceSaver Compression Bag", 15, 40, "Compression bags for reducing bulk and organizing gear efficiently in luggage"],
        "SECURITY": ["SafeGuard Travel Security", 10, 100, "Travel security items including locks, RFID wallets, and anti-theft devices for safe traveling"],  
        "TRAVEL ACCESSORIES": ["Global Travel Accessory Kit", 5, 80, "Essential travel accessories including adapters, chargers, and convenience items for global travel"],  
        "TRAVEL BACKPACKS": ["Nomad Adventure Backpack", 30, 200, "Travel-specific backpacks with organizational features and carry-on compatibility for adventure travel"],  
        "TRAVEL PILLOWS": ["Comfort Cloud Travel Pillow", 10, 40, "Portable travel pillows and neck supports for comfortable rest during long journeys"]  
    },  
    "WATER SPORTS": {  
        "ACCESSORIES": ["Marine Adventure Accessories", 10, 100, "Water sports accessories including waterproof bags, boat fenders, and marine safety equipment"],  
        "DRY BAGS": ["WaterShield Dry Bag Set", 20, 80, "Waterproof dry bags in multiple sizes for protecting gear during water activities"],
        "CANOES": ["Explorer Classic Canoe", 300, 1200, "High-quality canoes for recreational paddling, touring, and whitewater adventures with various hull designs"],  
        "KAYAKS": ["River Runner Kayak", 200, 1000, "Recreational and touring kayaks for flatwater and whitewater paddling with stability and performance features"],  
        "INFLATABLE KAYAKS": ["PackRaft Explorer Kayak", 150, 600, "Portable inflatable kayaks for easy transport and storage with reliable performance"],
        "OTHER": ["Water Sports Gear Pack", 10, 100, "Miscellaneous water sports equipment including flotation devices and specialty marine gear"],  
        "PADDLES": ["Precision Paddle Pro", 20, 150, "Lightweight paddles for kayaking and canoeing with ergonomic grips and efficient blade designs"],  
        "RASH GUARDS": ["Sun Shield Rash Guard", 20, 80, "UV-protective rash guards and swim shirts for sun protection during water sports activities"],  
        "RODS & REELS": ["AngleMaster Rod & Reel", 30, 200, "Fishing rods and reels for freshwater and saltwater angling with various action and weight ratings"],  
        "FLY FISHING GEAR": ["StreamCast Fly Fishing Kit", 80, 300, "Complete fly fishing setup with rods, reels, and tackle for freshwater and saltwater fly fishing"],
        "SAFETY GEAR": ["AquaSafe Safety Kit", 20, 100, "Water safety equipment including life jackets, whistles, and emergency signaling devices"],  
        "SURF ACCESSORIES": ["Wave Rider Accessory Kit", 10, 100, "Surfing accessories including wax, leashes, fins, and board care products"],  
        "SURFBOARDS": ["Storm Rider Surfboard", 200, 800, "High-performance surfboards for various skill levels and wave conditions with modern shaping technology"],  
        "STAND-UP PADDLEBOARDS": ["Balance Master SUP Board", 250, 700, "Stable stand-up paddleboards for recreational paddling and fitness on calm waters"],
        "TACKLE": ["Pro Angler Tackle Box", 5, 50, "Fishing tackle including lures, hooks, sinkers, and terminal tackle for successful angling"],  
        "WADERS": ["StreamWalk Pro Waders", 50, 200, "Waterproof waders for fishing and water work with breathable materials and reinforced construction"],  
        "WETSUITS": ["ThermalFlex Wetsuit", 50, 300, "Thermal protection wetsuits for diving, surfing, and water sports with flexible neoprene construction"]  
    },  
    "WINTER SPORTS": {  
        "ACCESSORIES": ["Winter Sport Essentials", 10, 100, "Winter sports accessories including goggles, gloves, face masks, and gear maintenance items"],  
        "GOGGLES": ["ClearVision Snow Goggles", 30, 150, "Anti-fog snow goggles with UV protection and interchangeable lenses for varying conditions"],
        "GLOVES": ["ThermalGrip Winter Gloves", 20, 80, "Insulated winter gloves with dexterity and warmth for skiing and snowboarding"],
        "BINDINGS": ["FlexRide Snowboard Bindings", 80, 300, "Snowboard bindings with responsive performance and comfort features for all riding styles"],  
        "HELMETS": ["Snow Guardian Helmet", 40, 150, "Winter sports helmets with impact protection and ventilation for skiing and snowboarding safety"],  
        "OTHER": ["Snow Sports Gear Kit", 10, 100, "Miscellaneous winter sports equipment including tuning tools and specialty snow gear"],  
        "POLES": ["Balance Pro Ski Poles", 20, 100, "Ski poles with ergonomic grips and lightweight construction for balance and propulsion"],  
        "SKI BINDINGS": ["Precision Release Ski Bindings", 100, 300, "Alpine ski bindings with safety release mechanisms and performance-oriented designs"],  
        "SKI BOOTS": ["Alpine Control Ski Boots", 150, 500, "Precision ski boots with custom fit options and responsive performance for various skiing styles"],  
        "SKI POLES": ["Carbon Elite Ski Poles", 30, 120, "Lightweight ski poles with comfortable grips and durable construction for alpine and touring skiing"],  
        "SKIS": ["Mountain Master Skis", 200, 800, "High-performance skis for various snow conditions and skiing styles with advanced construction technology"],  
        "ALL-MOUNTAIN SKIS": ["Versatile Peak Skis", 250, 700, "All-mountain skis designed for versatile performance across various terrain and snow conditions"],
        "SNOWBOARD BOOTS": ["BoardRide Pro Boots", 100, 300, "Comfortable snowboard boots with heat-moldable liners and responsive flex patterns"],  
        "SNOWBOARDS": ["Freestyle Terrain Board", 200, 800, "All-mountain and freestyle snowboards with versatile performance characteristics for various terrains"],  
        "SPLITBOARDS": ["Backcountry Split Board", 300, 900, "Splitboards for backcountry snowboarding with touring capability and downhill performance"],
        "SNOWSHOES": ["Backcountry Trek Snowshoes", 50, 200, "Lightweight snowshoes with efficient traction systems for winter hiking and backcountry exploration"]  
    }  
}

regions = ['AFRICA', 'ASIA-PACIFIC', 'CHINA', 'EUROPE', 'LATIN AMERICA', 'MIDDLE EAST', 'NORTH AMERICA']

# Weighted distribution for regions - North America highest, Europe second, China third
region_weights = {
    'NORTH AMERICA': 30,    # Highest sales
    'EUROPE': 25,           # Second highest
    'CHINA': 20,            # Third highest
    'ASIA-PACIFIC': 8,      # Lower sales
    'LATIN AMERICA': 7,     # Lower sales
    'MIDDLE EAST': 6,       # Lower sales
    'AFRICA': 4             # Lowest sales
}

def weighted_region_choice():
    """Choose a region based on weighted distribution"""
    regions_list = list(region_weights.keys())
    weights_list = list(region_weights.values())
    return random.choices(regions_list, weights=weights_list, k=1)[0]

def generate_phone_number(region=None):
    """Generate a realistic phone number based on region"""
    # Regional phone number formats and country codes
    region_phone_formats = {
        'NORTH AMERICA': {
            'country_codes': ['+1'],
            'format': lambda: f"+1-{random.randint(200, 999)}-{random.randint(200, 999)}-{random.randint(1000, 9999)}"
        },
        'EUROPE': {
            'country_codes': ['+33', '+49', '+44', '+39', '+34', '+46', '+31', '+41'],
            'format': lambda code: f"{code}-{random.randint(1, 9)}{random.randint(10000000, 99999999)}"
        },
        'CHINA': {
            'country_codes': ['+86'],
            'format': lambda: f"+86-{random.choice([130, 131, 132, 133, 134, 135, 136, 137, 138, 139])}-{random.randint(1000, 9999)}-{random.randint(1000, 9999)}"
        },
        'ASIA-PACIFIC': {
            'country_codes': ['+81', '+82', '+65', '+60', '+66', '+84', '+63'],
            'format': lambda code: f"{code}-{random.randint(10000000, 99999999)}"
        },
        'LATIN AMERICA': {
            'country_codes': ['+52', '+54', '+55', '+56', '+57', '+58', '+51'],
            'format': lambda code: f"{code}-{random.randint(1000000000, 9999999999)}"
        },
        'MIDDLE EAST': {
            'country_codes': ['+971', '+966', '+973', '+974', '+965', '+968', '+20'],
            'format': lambda code: f"{code}-{random.randint(100000000, 999999999)}"
        },
        'AFRICA': {
            'country_codes': ['+27', '+234', '+254', '+233', '+212', '+213', '+216'],
            'format': lambda code: f"{code}-{random.randint(100000000, 999999999)}"
        }
    }
    
    if region and region in region_phone_formats:
        region_info = region_phone_formats[region]
        country_code = random.choice(region_info['country_codes'])
        
        if region == 'NORTH AMERICA':
            return region_info['format']()
        elif region == 'CHINA':
            return region_info['format']()
        else:
            return region_info['format'](country_code)
    else:
        # Default to North American format if region not specified
        return f"+1-{random.randint(200, 999)}-{random.randint(200, 999)}-{random.randint(1000, 9999)}"

def create_database_schema(conn):
    """Create database tables and indexes"""
    cursor = conn.cursor()
    
    # Create customers table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS customers (
            customer_id INTEGER PRIMARY KEY,
            first_name TEXT NOT NULL,
            last_name TEXT NOT NULL,
            email TEXT UNIQUE NOT NULL,
            phone TEXT,
            region TEXT
        )
    """)
    
    # Create products table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS products (
            product_id INTEGER PRIMARY KEY,
            product_name TEXT NOT NULL,
            main_category TEXT NOT NULL,
            product_type TEXT NOT NULL,
            base_price REAL NOT NULL,
            product_description TEXT NOT NULL
        )
    """)
    
    # Create orders table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS orders (
            order_id INTEGER PRIMARY KEY,
            customer_id INTEGER NOT NULL,
            product_id INTEGER NOT NULL,
            quantity INTEGER NOT NULL,
            unit_price REAL NOT NULL,
            discount_percent INTEGER DEFAULT 0,
            discount_amount REAL DEFAULT 0,
            total_amount REAL NOT NULL,
            order_date DATE NOT NULL,
            FOREIGN KEY (customer_id) REFERENCES customers (customer_id),
            FOREIGN KEY (product_id) REFERENCES products (product_id)
        )
    """)
    
    # Create comprehensive performance indexes
    print("Creating performance indexes...")
    
    # Basic indexes for foreign keys and common filters
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_customers_region ON customers(region)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_products_category ON products(main_category)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_products_type ON products(product_type)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_orders_customer ON orders(customer_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_orders_product ON orders(product_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_orders_date ON orders(order_date)")
    
    # Composite indexes for common query patterns
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_orders_date_total ON orders(order_date, total_amount)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_orders_customer_date ON orders(customer_id, order_date)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_orders_product_date ON orders(product_id, order_date)")
    
    # Covering indexes for aggregation queries
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_orders_covering_sales ON orders(order_date, customer_id, total_amount, quantity)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_products_covering ON products(main_category, product_type, product_id, base_price)")
    
    # Index for discount analysis
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_orders_discount ON orders(discount_percent, total_amount)")
    
    # Email index for customer lookups
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_customers_email ON customers(email)")
    
    # Composite index for regional product analysis
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_customer_region_id ON customers(region, customer_id)")
    
    # Year-based indexes for time series analysis
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_orders_year ON orders(substr(order_date, 1, 4))")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_orders_year_month ON orders(substr(order_date, 1, 7))")
    
    conn.commit()
    print("Performance indexes created successfully!")
    print("Database schema created successfully!")

def insert_customers(conn, num_customers=100000):
    """Insert customer data into the database"""
    cursor = conn.cursor()
    
    print(f"Generating {num_customers:,} customers...")
    
    # Prepare customer data in batches for better performance
    batch_size = 1000
    customers_data = []
    
    for i in range(1, num_customers + 1):
        first_name = fake.first_name().replace("'", "''")  # Escape single quotes
        last_name = fake.last_name().replace("'", "''")
        # Use customer ID to ensure unique emails
        email = f"{first_name.lower()}.{last_name.lower()}.{i}@example.com"
        region = weighted_region_choice()  # Use weighted selection
        phone = generate_phone_number(region)  # Generate region-specific phone number
        
        customers_data.append((i, first_name, last_name, email, phone, region))
        
        # Insert in batches
        if len(customers_data) >= batch_size:
            cursor.executemany(
                "INSERT INTO customers (customer_id, first_name, last_name, email, phone, region) VALUES (?, ?, ?, ?, ?, ?)",
                customers_data
            )
            customers_data = []
            if i % 10000 == 0:
                print(f"  Inserted {i:,} customers...")
                conn.commit()
    
    # Insert remaining customers
    if customers_data:
        cursor.executemany(
            "INSERT INTO customers (customer_id, first_name, last_name, email, phone, region) VALUES (?, ?, ?, ?, ?, ?)",
            customers_data
        )
    
    conn.commit()
    print(f"Successfully inserted {num_customers:,} customers!")

def insert_products(conn):
    """Insert product data into the database"""
    cursor = conn.cursor()
    
    print("Generating products...")
    
    products_data = []
    product_id = 1
    
    for main_category, subcategories in main_categories.items():
        for product_type, product_info in subcategories.items():
            product_name = product_info[0]    # First element is the product name
            price_range = product_info[1:3]   # Second and third elements are min/max price
            description = product_info[3]     # Fourth element is the description
            
            # Generate only one product per product type
            base_price = random.randint(price_range[0], price_range[1])
            
            products_data.append((product_id, product_name, main_category, product_type, base_price, description))
            product_id += 1
    
    cursor.executemany(
        "INSERT INTO products (product_id, product_name, main_category, product_type, base_price, product_description) VALUES (?, ?, ?, ?, ?, ?)",
        products_data
    )
    
    conn.commit()
    print(f"Successfully inserted {len(products_data):,} products!")
    return product_id - 1

def get_customer_region(conn, customer_id):
    """Get the region for a specific customer"""
    cursor = conn.cursor()
    cursor.execute("SELECT region FROM customers WHERE customer_id = ?", (customer_id,))
    result = cursor.fetchone()
    return result[0] if result else 'NORTH AMERICA'

def get_region_multipliers(region):
    """Get order count and value multipliers based on region"""
    multipliers = {
        'NORTH AMERICA': {'orders': 1.5, 'value': 1.3},    # 50% more orders, 30% higher values
        'EUROPE': {'orders': 1.3, 'value': 1.2},           # 30% more orders, 20% higher values  
        'CHINA': {'orders': 1.2, 'value': 1.1},            # 20% more orders, 10% higher values
        'ASIA-PACIFIC': {'orders': 1.0, 'value': 1.0},     # Base level
        'LATIN AMERICA': {'orders': 0.9, 'value': 0.95},   # 10% fewer orders, 5% lower values
        'MIDDLE EAST': {'orders': 0.8, 'value': 0.9},      # 20% fewer orders, 10% lower values
        'AFRICA': {'orders': 0.7, 'value': 0.85}           # 30% fewer orders, 15% lower values
    }
    return multipliers.get(region, {'orders': 1.0, 'value': 1.0})

def get_yearly_weight(year):
    """Get the weight for each year to create growth pattern"""
    # Base year 2020 = 1.0, then 10% growth each year except 2023 (-5%)
    year_weights = {
        2020: 1.0,      # Base year
        2021: 1.1,      # 10% growth
        2022: 1.21,     # 10% growth (1.1 * 1.1)
        2023: 1.15,     # 5% decline from 2022 (1.21 * 0.95)
        2024: 1.265,    # 10% growth from 2023 (1.15 * 1.1)
        2025: 1.392,    # 10% growth (1.265 * 1.1)
        2026: 1.531     # 10% growth (1.392 * 1.1)
    }
    return year_weights.get(year, 1.0)

def weighted_year_choice():
    """Choose a year based on growth pattern weights"""
    years = [2020, 2021, 2022, 2023, 2024, 2025, 2026]
    weights = [get_yearly_weight(year) for year in years]
    return random.choices(years, weights=weights, k=1)[0]

def insert_orders(conn, num_customers=100000, max_product_id=1):
    """Insert order data into the database"""
    cursor = conn.cursor()
    
    print(f"Generating orders for {num_customers:,} customers...")
    print("Creating sales growth pattern: 10% YoY growth, except 2023 (-5%)")
    
    batch_size = 1000
    orders_data = []
    order_id = 1
    total_orders = 0
    
    for customer_id in range(1, num_customers + 1):
        # Get customer region for order adjustments
        region = get_customer_region(conn, customer_id)
        multipliers = get_region_multipliers(region)
        
        # Adjust number of orders based on region (2-8 base, modified by region)
        base_orders = random.randint(2, 8)
        num_orders = max(1, int(base_orders * multipliers['orders']))
        
        for _ in range(num_orders):
            product_id = random.randint(1, max_product_id)
            quantity = random.randint(1, 5)
            
            # Generate weighted year and random date within that year
            order_year = weighted_year_choice()
            start_of_year = datetime.date(order_year, 1, 1)
            end_of_year = datetime.date(order_year, 12, 31)
            days_in_year = (end_of_year - start_of_year).days
            random_days = random.randint(0, days_in_year)
            order_date = start_of_year + datetime.timedelta(days=random_days)
            
            # Use seasonal selection based on order date and customer region
            order_month = order_date.month
            main_category = choose_seasonal_category(order_month, region)
            product_type = choose_seasonal_product_type(main_category, order_month, region)
            product_info = main_categories[main_category][product_type]
            price_range = product_info[1:3]  # Second and third elements are min/max price
            
            # Apply regional value multiplier to unit price
            base_unit_price = random.randint(price_range[0], price_range[1])
            unit_price = base_unit_price * multipliers['value']
            
            discount_percent = random.randint(0, 15)  # 0-15% discount
            discount_amount = round((unit_price * quantity * discount_percent) / 100, 2)
            
            total_amount = (unit_price * quantity) - discount_amount
            
            orders_data.append((
                order_id, customer_id, product_id, quantity, unit_price,
                discount_percent, discount_amount, total_amount, order_date.isoformat()
            ))
            
            order_id += 1
            total_orders += 1
            
            # Insert in batches
            if len(orders_data) >= batch_size:
                cursor.executemany(
                    """INSERT INTO orders (order_id, customer_id, product_id, quantity, unit_price,
                       discount_percent, discount_amount, total_amount, order_date) 
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    orders_data
                )
                orders_data = []
                
                if total_orders % 50000 == 0:
                    print(f"  Inserted {total_orders:,} orders...")
                    conn.commit()
    
    # Insert remaining orders
    if orders_data:
        cursor.executemany(
            """INSERT INTO orders (order_id, customer_id, product_id, quantity, unit_price,
               discount_percent, discount_amount, total_amount, order_date) 
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            orders_data
        )
    
    conn.commit()
    print(f"Successfully inserted {total_orders:,} orders!")

def generate_sqlite_database(db_path="customer_sales.db", num_customers=50000):
    """Generate complete SQLite database"""
    
    # Remove existing database if it exists
    if os.path.exists(db_path):
        os.remove(db_path)
        print(f"Removed existing database: {db_path}")
    
    # Create new database connection
    conn = sqlite3.connect(db_path)
    
    try:
        print(f"Creating SQLite database: {db_path}")
        print("=" * 50)
        
        # Create schema
        create_database_schema(conn)
        
        # Insert data
        insert_customers(conn, num_customers)
        max_product_id = insert_products(conn)
        insert_orders(conn, num_customers, max_product_id)
        
        # Get final statistics
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM customers")
        customer_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM products")
        product_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM orders")
        order_count = cursor.fetchone()[0]
        
        print("=" * 50)
        print("Database generation complete!")
        print(f"Database file: {db_path}")
        print(f"File size: {os.path.getsize(db_path) / (1024*1024):.1f} MB")
        print(f"Customers: {customer_count:,}")
        print(f"Products: {product_count:,}")
        print(f"Orders: {order_count:,}")
        
        # Run VACUUM to optimize database
        print("Optimizing database...")
        conn.execute("VACUUM")
        print(f"Optimized file size: {os.path.getsize(db_path) / (1024*1024):.1f} MB")
        
        # Analyze tables for better query planning
        print("Analyzing tables for optimal query planning...")
        conn.execute("ANALYZE")
        conn.commit()
        
        # Verify data and show sample statistics
        verify_database_contents(conn)
        
    except Exception as e:
        print(f"Error generating database: {e}")
        raise
    finally:
        conn.close()

def verify_database_contents(conn):
    """Verify database contents and show key statistics"""
    cursor = conn.cursor()
    
    print("\n" + "=" * 60)
    print("DATABASE VERIFICATION & STATISTICS")
    print("=" * 60)
    
    # Regional distribution verification
    print("\n📍 REGIONAL SALES DISTRIBUTION:")
    cursor.execute("""
        SELECT c.region, 
               COUNT(o.order_id) as orders,
               printf('$%.1fM', SUM(o.total_amount)/1000000.0) as revenue,
               printf('%.1f%%', 100.0 * COUNT(o.order_id) / (SELECT COUNT(*) FROM orders)) as order_pct
        FROM customers c 
        JOIN orders o ON c.customer_id = o.customer_id 
        GROUP BY c.region 
        ORDER BY SUM(o.total_amount) DESC
    """)
    
    print("   Region              Orders     Revenue    % of Orders")
    print("   " + "-" * 50)
    for row in cursor.fetchall():
        print(f"   {row[0]:<18} {row[1]:>8,} {row[2]:>10} {row[3]:>10}")
    
    # Year-over-year growth verification
    print("\n📈 YEAR-OVER-YEAR GROWTH PATTERN:")
    cursor.execute("""
        SELECT SUBSTR(order_date, 1, 4) as year,
               COUNT(*) as orders,
               printf('$%.1fM', SUM(total_amount)/1000000.0) as revenue,
               LAG(SUM(total_amount)) OVER (ORDER BY SUBSTR(order_date, 1, 4)) as prev_revenue
        FROM orders 
        GROUP BY SUBSTR(order_date, 1, 4)
        ORDER BY year
    """)
    
    print("   Year    Orders     Revenue    Growth")
    print("   " + "-" * 35)
    results = cursor.fetchall()
    for i, row in enumerate(results):
        year, orders, revenue, prev_revenue = row
        if prev_revenue:
            growth = ((float(revenue.replace('$', '').replace('M', '')) - 
                      float(prev_revenue)) / float(prev_revenue)) * 100
            growth_str = f"{growth:+.1f}%"
        else:
            growth_str = "Base"
        print(f"   {year}   {orders:>8,} {revenue:>10} {growth_str:>8}")
    
    # Product category distribution
    print("\n🛍️  TOP PRODUCT CATEGORIES:")
    cursor.execute("""
        SELECT p.main_category,
               COUNT(o.order_id) as orders,
               printf('$%.1fM', SUM(o.total_amount)/1000000.0) as revenue
        FROM products p
        JOIN orders o ON p.product_id = o.product_id
        GROUP BY p.main_category
        ORDER BY SUM(o.total_amount) DESC
        LIMIT 5
    """)
    
    print("   Category             Orders     Revenue")
    print("   " + "-" * 40)
    for row in cursor.fetchall():
        print(f"   {row[0]:<18} {row[1]:>8,} {row[2]:>10}")
    
    # Database performance test
    print("\n⚡ QUERY PERFORMANCE TEST:")
    import time
    
    test_queries = [
        ("Regional aggregation", "SELECT region, COUNT(*), SUM(total_amount) FROM customers c JOIN orders o ON c.customer_id = o.customer_id GROUP BY region"),
        ("Yearly trend", "SELECT SUBSTR(order_date, 1, 4), COUNT(*), SUM(total_amount) FROM orders GROUP BY SUBSTR(order_date, 1, 4)"),
        ("Customer order history", "SELECT customer_id, COUNT(*), MAX(order_date) FROM orders WHERE customer_id <= 100 GROUP BY customer_id"),
    ]
    
    for query_name, query in test_queries:
        start_time = time.time()
        cursor.execute(query)
        results = cursor.fetchall()
        elapsed = time.time() - start_time
        print(f"   {query_name:<20}: {elapsed:.3f}s ({len(results)} rows)")
    
    # Index verification
    print("\n🗂️  DATABASE INDEXES:")
    cursor.execute("SELECT name FROM sqlite_master WHERE type='index' AND name NOT LIKE 'sqlite_%' ORDER BY name")
    indexes = [row[0] for row in cursor.fetchall()]
    print(f"   Created {len(indexes)} performance indexes")
    
    # Final summary
    cursor.execute("SELECT COUNT(*) FROM customers")
    customers = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM products")  
    products = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM orders")
    orders = cursor.fetchone()[0]
    cursor.execute("SELECT SUM(total_amount) FROM orders")
    total_revenue = cursor.fetchone()[0]
    
    print("\n✅ DATABASE SUMMARY:")
    print(f"   Customers:     {customers:>8,}")
    print(f"   Products:      {products:>8,}")
    print(f"   Orders:        {orders:>8,}")
    print(f"   Total Revenue: ${total_revenue/1000000:.1f}M")
    print(f"   Avg Order:     ${total_revenue/orders:.2f}")
    print(f"   Orders/Customer: {orders/customers:.1f}")

def test_query_performance(db_path="../customer_sales.db"):
    """Test comprehensive query performance on the generated database"""
    
    print("\n" + "=" * 60)
    print("COMPREHENSIVE QUERY PERFORMANCE TEST")
    print("=" * 60)
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    test_queries = [
        ("Regional Sales Summary", """
            SELECT c.region, 
                   COUNT(o.order_id) as orders,
                   SUM(o.total_amount) as revenue,
                   AVG(o.total_amount) as avg_order
            FROM customers c 
            JOIN orders o ON c.customer_id = o.customer_id 
            GROUP BY c.region 
            ORDER BY revenue DESC
        """),
        
        ("Yearly Revenue Trend", """
            SELECT SUBSTR(order_date, 1, 4) as year,
                   SUM(total_amount) as revenue,
                   COUNT(*) as orders,
                   AVG(total_amount) as avg_order
            FROM orders 
            GROUP BY SUBSTR(order_date, 1, 4)
            ORDER BY year
        """),
        
        ("Monthly Revenue for 2023", """
            SELECT SUBSTR(order_date, 1, 7) as month,
                   SUM(total_amount) as revenue,
                   COUNT(*) as orders
            FROM orders 
            WHERE order_date LIKE '2023%'
            GROUP BY SUBSTR(order_date, 1, 7)
            ORDER BY month
        """),
        
        ("Top Products by Category", """
            SELECT p.main_category,
                   p.product_type,
                   COUNT(o.order_id) as orders,
                   SUM(o.total_amount) as revenue
            FROM products p
            JOIN orders o ON p.product_id = o.product_id
            GROUP BY p.main_category, p.product_type
            ORDER BY revenue DESC
            LIMIT 10
        """),
        
        ("Customer Purchase Patterns", """
            SELECT c.region,
                   COUNT(DISTINCT o.customer_id) as customers,
                   AVG(customer_orders) as avg_orders_per_customer,
                   AVG(customer_revenue) as avg_revenue_per_customer
            FROM customers c
            JOIN (
                SELECT customer_id, 
                       COUNT(*) as customer_orders,
                       SUM(total_amount) as customer_revenue
                FROM orders 
                GROUP BY customer_id
            ) o ON c.customer_id = o.customer_id
            GROUP BY c.region
            ORDER BY avg_revenue_per_customer DESC
        """),
        
        ("Discount Effectiveness", """
            SELECT 
                CASE 
                    WHEN discount_percent = 0 THEN 'No Discount'
                    WHEN discount_percent <= 5 THEN '1-5%'
                    WHEN discount_percent <= 10 THEN '6-10%'
                    ELSE '11%+'
                END as discount_range,
                COUNT(*) as orders,
                AVG(total_amount) as avg_order_value,
                SUM(total_amount) as total_revenue
            FROM orders
            GROUP BY discount_range
            ORDER BY avg_order_value DESC
        """),
    ]
    
    import time
    
    for query_name, query in test_queries:
        print(f"\n🔍 {query_name}:")
        start_time = time.time()
        
        cursor.execute(query)
        results = cursor.fetchall()
        
        elapsed = time.time() - start_time
        print(f"   Execution time: {elapsed:.3f} seconds")
        print(f"   Rows returned: {len(results)}")
        
        # Show first few results
        if results:
            print("   Sample results:")
            for i, row in enumerate(results[:3]):
                formatted_row = []
                for val in row:
                    if isinstance(val, float):
                        if val > 1000000:
                            formatted_row.append(f"${val/1000000:.1f}M")
                        elif val > 1000:
                            formatted_row.append(f"${val:,.0f}")
                        else:
                            formatted_row.append(f"{val:.2f}")
                    else:
                        formatted_row.append(str(val))
                print(f"     {formatted_row}")
            if len(results) > 3:
                print(f"     ... and {len(results) - 3} more rows")
    
    conn.close()
    
    print("\n🎉 Performance test complete!")
    print("\nRecommended query patterns for best performance:")
    print("• Use indexed columns in WHERE clauses")
    print("• Leverage covering indexes for SELECT columns")
    print("• Use SUBSTR(order_date, 1, 4) for year queries")
    print("• Use SUBSTR(order_date, 1, 7) for year-month queries")
    print("• Join on indexed foreign keys (customer_id, product_id)")

def get_seasonal_category_weights(month, region):
    """Get category weights based on season and region"""
    # Define seasons by month (Northern Hemisphere for most regions)
    if region in ['AFRICA']:
        # Southern Hemisphere - opposite seasons
        if month in [12, 1, 2]:  # Summer
            season = 'summer'
        elif month in [3, 4, 5]:  # Autumn
            season = 'autumn'
        elif month in [6, 7, 8]:  # Winter
            season = 'winter'
        else:  # Spring
            season = 'spring'
    else:
        # Northern Hemisphere
        if month in [6, 7, 8]:  # Summer
            season = 'summer'
        elif month in [9, 10, 11]:  # Autumn
            season = 'autumn'
        elif month in [12, 1, 2]:  # Winter
            season = 'winter'
        else:  # Spring
            season = 'spring'
    
    # Base weights (equal distribution)
    base_weights = {
        "APPAREL": 1.0,
        "CAMPING & HIKING": 1.0,
        "CLIMBING": 1.0,
        "FOOTWEAR": 1.0,
        "TRAVEL": 1.0,
        "WATER SPORTS": 1.0,
        "WINTER SPORTS": 1.0
    }
    
    # Seasonal modifiers by region
    seasonal_modifiers = {
        'NORTH AMERICA': {
            'winter': {"WINTER SPORTS": 2.5, "APPAREL": 1.5, "CLIMBING": 0.7, "WATER SPORTS": 0.3, "CAMPING & HIKING": 0.5},
            'spring': {"CAMPING & HIKING": 1.8, "FOOTWEAR": 1.5, "CLIMBING": 1.4, "TRAVEL": 1.3, "WINTER SPORTS": 0.6},
            'summer': {"WATER SPORTS": 2.2, "CAMPING & HIKING": 2.0, "TRAVEL": 1.8, "FOOTWEAR": 1.3, "WINTER SPORTS": 0.2},
            'autumn': {"APPAREL": 1.6, "FOOTWEAR": 1.5, "CLIMBING": 1.3, "CAMPING & HIKING": 1.2, "WINTER SPORTS": 0.8}
        },
        'EUROPE': {
            'winter': {"WINTER SPORTS": 3.0, "APPAREL": 1.6, "CLIMBING": 0.8, "WATER SPORTS": 0.2, "CAMPING & HIKING": 0.4},
            'spring': {"CAMPING & HIKING": 1.9, "FOOTWEAR": 1.6, "CLIMBING": 1.5, "TRAVEL": 1.4, "WINTER SPORTS": 0.5},
            'summer': {"WATER SPORTS": 2.0, "CAMPING & HIKING": 2.2, "TRAVEL": 2.0, "FOOTWEAR": 1.4, "WINTER SPORTS": 0.1},
            'autumn': {"APPAREL": 1.7, "FOOTWEAR": 1.6, "CLIMBING": 1.4, "CAMPING & HIKING": 1.1, "WINTER SPORTS": 0.7}
        },
        'CHINA': {
            'winter': {"WINTER SPORTS": 2.0, "APPAREL": 1.4, "CLIMBING": 0.9, "WATER SPORTS": 0.4, "CAMPING & HIKING": 0.6},
            'spring': {"CAMPING & HIKING": 1.7, "FOOTWEAR": 1.4, "CLIMBING": 1.3, "TRAVEL": 1.3, "WINTER SPORTS": 0.7},
            'summer': {"WATER SPORTS": 1.8, "CAMPING & HIKING": 1.9, "TRAVEL": 1.7, "FOOTWEAR": 1.2, "WINTER SPORTS": 0.3},
            'autumn': {"APPAREL": 1.5, "FOOTWEAR": 1.4, "CLIMBING": 1.2, "CAMPING & HIKING": 1.1, "WINTER SPORTS": 0.8}
        },
        'ASIA-PACIFIC': {
            'winter': {"WINTER SPORTS": 1.5, "APPAREL": 1.3, "WATER SPORTS": 0.6, "CAMPING & HIKING": 0.8},
            'spring': {"CAMPING & HIKING": 1.6, "FOOTWEAR": 1.3, "CLIMBING": 1.2, "TRAVEL": 1.4},
            'summer': {"WATER SPORTS": 2.5, "CAMPING & HIKING": 1.8, "TRAVEL": 1.6, "FOOTWEAR": 1.2, "WINTER SPORTS": 0.2},
            'autumn': {"APPAREL": 1.4, "FOOTWEAR": 1.3, "CLIMBING": 1.1, "TRAVEL": 1.2}
        },
        'LATIN AMERICA': {
            'winter': {"WINTER SPORTS": 0.8, "APPAREL": 1.2, "WATER SPORTS": 1.4, "CAMPING & HIKING": 1.1},
            'spring': {"CAMPING & HIKING": 1.5, "FOOTWEAR": 1.3, "CLIMBING": 1.2, "TRAVEL": 1.3},
            'summer': {"WATER SPORTS": 2.0, "CAMPING & HIKING": 1.7, "TRAVEL": 1.5, "FOOTWEAR": 1.2},
            'autumn': {"APPAREL": 1.3, "FOOTWEAR": 1.2, "CLIMBING": 1.1, "TRAVEL": 1.1}
        },
        'MIDDLE EAST': {
            'winter': {"WINTER SPORTS": 0.5, "APPAREL": 1.3, "WATER SPORTS": 0.8, "CAMPING & HIKING": 1.2},
            'spring': {"CAMPING & HIKING": 1.6, "FOOTWEAR": 1.4, "CLIMBING": 1.3, "TRAVEL": 1.4},
            'summer': {"WATER SPORTS": 1.8, "CAMPING & HIKING": 1.2, "TRAVEL": 1.6, "APPAREL": 0.8},
            'autumn': {"APPAREL": 1.4, "FOOTWEAR": 1.3, "CLIMBING": 1.2, "TRAVEL": 1.2}
        },
        'AFRICA': {
            # Southern Hemisphere - seasons are opposite
            'winter': {"WINTER SPORTS": 0.3, "APPAREL": 1.4, "WATER SPORTS": 0.9, "CAMPING & HIKING": 1.3},
            'spring': {"CAMPING & HIKING": 1.4, "FOOTWEAR": 1.2, "CLIMBING": 1.1, "TRAVEL": 1.2},
            'summer': {"WATER SPORTS": 2.2, "CAMPING & HIKING": 1.8, "TRAVEL": 1.5, "FOOTWEAR": 1.3},
            'autumn': {"APPAREL": 1.3, "FOOTWEAR": 1.2, "CLIMBING": 1.1, "TRAVEL": 1.1}
        }
    }
    
    # Apply seasonal modifiers
    weights = base_weights.copy()
    if region in seasonal_modifiers and season in seasonal_modifiers[region]:
        for category, modifier in seasonal_modifiers[region][season].items():
            if category in weights:
                weights[category] *= modifier
    
    return weights

def get_seasonal_product_type_weights(main_category, month, region):
    """Get product type weights within a category based on season and region"""
    # Define season
    if region in ['AFRICA']:
        if month in [12, 1, 2]:
            season = 'summer'
        elif month in [3, 4, 5]:
            season = 'autumn'
        elif month in [6, 7, 8]:
            season = 'winter'
        else:
            season = 'spring'
    else:
        if month in [6, 7, 8]:
            season = 'summer'
        elif month in [9, 10, 11]:
            season = 'autumn'
        elif month in [12, 1, 2]:
            season = 'winter'
        else:
            season = 'spring'
    
    # Product type seasonal preferences
    seasonal_product_preferences = {
        "APPAREL": {
            'winter': {"JACKETS & VESTS": 2.0, "FLEECE JACKETS": 1.8, "SOFTSHELL JACKETS": 1.5, "UNDERWEAR & BASE LAYERS": 1.6, "GLOVES": 2.0, "HATS & CAPS": 1.8},
            'summer': {"SHORTS": 2.2, "SHIRTS": 1.8, "POLO SHIRTS": 1.6, "RAIN JACKETS": 0.6, "FLEECE JACKETS": 0.4},
            'spring': {"RAIN JACKETS": 1.8, "SOFTSHELL JACKETS": 1.5, "PANTS & SHORTS": 1.4},
            'autumn': {"JACKETS & VESTS": 1.6, "FLEECE JACKETS": 1.5, "PANTS & SHORTS": 1.3}
        },
        "FOOTWEAR": {
            'winter': {"WINTER BOOTS": 2.5, "HIKING BOOTS": 1.2, "SANDALS": 0.3, "WATER SHOES": 0.2},
            'summer': {"SANDALS": 2.0, "WATER SHOES": 1.8, "TRAIL SHOES": 1.5, "WINTER BOOTS": 0.2},
            'spring': {"HIKING BOOTS": 1.6, "TRAIL SHOES": 1.4, "LIGHTWEIGHT BOOTS": 1.3},
            'autumn': {"HIKING BOOTS": 1.5, "CASUAL SHOES": 1.3, "WINTER BOOTS": 1.2}
        },
        "WINTER SPORTS": {
            'winter': {"SKIS": 2.5, "SNOWBOARDS": 2.3, "SKI BOOTS": 2.2, "SNOWBOARD BOOTS": 2.0, "GOGGLES": 2.0, "GLOVES": 1.8, "HELMETS": 1.6},
            'summer': {"SKIS": 0.3, "SNOWBOARDS": 0.3, "SKI BOOTS": 0.2, "SNOWBOARD BOOTS": 0.2},
            'spring': {"SPLITBOARDS": 1.4, "ALL-MOUNTAIN SKIS": 1.2},
            'autumn': {"SKIS": 1.2, "SNOWBOARDS": 1.1, "ACCESSORIES": 1.3}
        },
        "WATER SPORTS": {
            'summer': {"SURFBOARDS": 2.0, "KAYAKS": 1.8, "CANOES": 1.7, "STAND-UP PADDLEBOARDS": 1.9, "WETSUITS": 1.6, "RASH GUARDS": 2.2},
            'winter': {"SURFBOARDS": 0.6, "KAYAKS": 0.7, "RASH GUARDS": 0.4, "STAND-UP PADDLEBOARDS": 0.5},
            'spring': {"KAYAKS": 1.4, "CANOES": 1.3, "INFLATABLE KAYAKS": 1.2},
            'autumn': {"WETSUITS": 1.3, "DRY BAGS": 1.2}
        },
        "CAMPING & HIKING": {
            'summer': {"FAMILY CAMPING TENTS": 2.2, "BACKPACKING TENTS": 1.8, "SLEEPING BAGS": 1.6, "CAMPING CHAIRS": 2.0, "STOVES": 1.7},
            'winter': {"FAMILY CAMPING TENTS": 0.5, "SLEEPING BAGS": 1.2, "STOVES": 1.3, "LANTERNS": 1.5},
            'spring': {"BACKPACKING TENTS": 1.5, "DAYPACKS": 1.4, "SLEEPING PADS": 1.3, "HEADLAMPS": 1.2},
            'autumn': {"SLEEPING BAGS": 1.4, "SHELTERS & TARPS": 1.3, "OVERNIGHT PACKS": 1.2}
        },
        "CLIMBING": {
            'summer': {"CLIMBING SHOES": 1.6, "HARNESSES": 1.5, "ROPES & SLINGS": 1.4, "HELMETS": 1.3},
            'winter': {"CRAMPONS": 2.5, "ICE AXES": 2.2, "MOUNTAINEERING BOOTS": 2.0, "AVALANCHE SAFETY": 2.8},
            'spring': {"CLIMBING SHOES": 1.3, "APPROACH SHOES": 1.4, "TRAINING EQUIPMENT": 1.2},
            'autumn': {"MOUNTAINEERING BOOTS": 1.4, "HELMETS": 1.2}
        },
        "TRAVEL": {
            'summer': {"CARRY-ONS": 1.8, "TRAVEL BACKPACKS": 1.6, "PACKING ORGANIZERS": 1.5, "TRAVEL ACCESSORIES": 1.4},
            'winter': {"CHECKED LUGGAGE": 1.3, "ROLLING DUFFELS": 1.2},
            'spring': {"TRAVEL BACKPACKS": 1.4, "COMPRESSION SACKS": 1.2},
            'autumn': {"DUFFEL BAGS": 1.3, "SECURITY": 1.2}
        }
    }
    
    # Get base weights (equal for all product types in category)
    product_types = list(main_categories[main_category].keys())
    weights = {product_type: 1.0 for product_type in product_types}
    
    # Apply seasonal modifiers
    if main_category in seasonal_product_preferences and season in seasonal_product_preferences[main_category]:
        for product_type, modifier in seasonal_product_preferences[main_category][season].items():
            if product_type in weights:
                weights[product_type] *= modifier
    
    return weights

def choose_seasonal_category(month, region):
    """Choose a category based on seasonal preferences"""
    weights = get_seasonal_category_weights(month, region)
    categories = list(weights.keys())
    weight_values = list(weights.values())
    return random.choices(categories, weights=weight_values, k=1)[0]

def choose_seasonal_product_type(main_category, month, region):
    """Choose a product type within a category based on seasonal preferences"""
    weights = get_seasonal_product_type_weights(main_category, month, region)
    product_types = list(weights.keys())
    weight_values = list(weights.values())
    return random.choices(product_types, weights=weight_values, k=1)[0]

if __name__ == "__main__":
    # Check if faker is available
    try:
        from faker import Faker
    except ImportError:
        print("Error: faker library not found. Please install it with: pip install faker")
        exit(1)
    
    import sys
    
    # Check for command line arguments
    if len(sys.argv) > 1 and sys.argv[1] == "--test-performance":
        # Test performance on existing database
        db_path = "../customer_sales.db"
        if os.path.exists(db_path):
            test_query_performance(db_path)
        else:
            print(f"Database not found: {db_path}")
            print("Run without arguments to generate the database first.")
    else:
        # Generate the database
        db_path = "../customer_sales.db"  # Save in the database directory
        generate_sqlite_database(db_path, num_customers=50000)
        
        print("\n" + "🎯" * 20)
        print("CUSTOMER SALES DATABASE GENERATED SUCCESSFULLY!")
        print("🎯" * 20)
        print(f"\nDatabase location: {db_path}")
        print(f"To test query performance: python {sys.argv[0]} --test-performance")
        print("\nThe database includes:")
        print("✅ 50,000 customers with realistic regional distribution")
        print("✅ 104 products across 7 categories with seasonal preferences")
        print("✅ ~200,000-400,000 orders with year-over-year growth patterns")
        print("✅ Seasonal ordering patterns by region (winter sports in winter, etc.)")
        print("✅ Comprehensive performance indexes for fast queries")
        print("✅ Regional sales hierarchy (North America > Europe > China)")
        print("✅ Business growth pattern: 10% YoY except 2023 (-5%)")
        print("✅ Date range: 2020-2026 for historical and future analysis")