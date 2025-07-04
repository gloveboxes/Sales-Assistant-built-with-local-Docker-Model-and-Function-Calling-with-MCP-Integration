"""
Azure OpenAI DALL-E 3 Image Generation Script
Generates images for products in product_data.json and updates the JSON with image file paths.
"""

import json
import os
import requests
import time
import re
from datetime import datetime
from typing import Dict, Any, Optional
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from project root
load_dotenv("../../../.env")

class DalleImageGenerator:
    def __init__(self):
        """Initialize the DALL-E image generator with Azure OpenAI credentials."""
        self.dalle_endpoint = os.getenv('dalle_endpoint')
        self.dalle_key = os.getenv('dalle_key')
        
        if not self.dalle_endpoint or not self.dalle_key:
            raise ValueError("Missing DALL-E endpoint or API key in environment variables")
        
        self.headers = {
            "Content-Type": "application/json",
            "api-key": self.dalle_key
        }        # Paths - relative to script location
        script_dir = Path(__file__).parent
        self.product_data_path = script_dir / "product_data.json"
        self.images_dir = script_dir / "images"
        
        # Ensure images directory exists
        self.images_dir.mkdir(exist_ok=True)
        
        # Load product data
        self.product_data = self.load_product_data()
        
    def load_product_data(self) -> Dict[str, Any]:
        """Load product data from JSON file."""
        try:
            with open(self.product_data_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except FileNotFoundError:
            raise FileNotFoundError(f"Product data file not found: {self.product_data_path}")
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON in product data file: {e}")
    
    def save_product_data(self):
        """Save updated product data back to JSON file."""
        try:
            with open(self.product_data_path, 'w', encoding='utf-8') as f:
                json.dump(self.product_data, f, indent=2, ensure_ascii=False)
            print(f"Product data saved to {self.product_data_path}")
        except Exception as e:
            print(f"Error saving product data: {e}")
    
    def create_safe_filename(self, product_name: str, category: str, subcategory: str) -> str:
        """Create a safe, unique filename for the image."""
        # Remove special characters and spaces
        safe_name = re.sub(r'[^\w\s-]', '', product_name.lower())
        safe_name = re.sub(r'[-\s]+', '_', safe_name)
        
        # Create unique filename with category and timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{category.lower()}_{subcategory.lower()}_{safe_name}_{timestamp}.png"
        
        return filename
    
    def generate_image(self, product: Dict[str, Any], category: str, subcategory: str) -> Optional[str]:
        """Generate an image using DALL-E 3 for a specific product."""
        
        image_prompt = f"""
A simple realistic image of a "{product['description']}", isolated on a white background, centered, with no shadows.
"""
        
        payload = {
            "prompt": image_prompt,
            "size": "1024x1024",
            "quality": "standard",
            "style": "vivid",
            "response_format": "url"
        }
        
        try:
            print(f"Generating image for: {product['name']}")
            if not self.dalle_endpoint:
                raise ValueError("DALL-E endpoint URL is not set.")
            response = requests.post(self.dalle_endpoint, headers=self.headers, json=payload)
            
            if response.status_code == 200:
                result = response.json()
                image_url = result['data'][0]['url']
                
                # Download and save the image
                filename = self.create_safe_filename(product['name'], category, subcategory)
                image_path = self.images_dir / filename
                
                # Download the image
                image_response = requests.get(image_url)
                if image_response.status_code == 200:
                    with open(image_path, 'wb') as f:
                        f.write(image_response.content)
                    
                    print(f"Image saved: {filename}")
                    return str(image_path)
                else:
                    print(f"Failed to download image: {image_response.status_code}")
                    return None
            else:
                print(f"DALL-E API error: {response.status_code} - {response.text}")
                return None
                
        except Exception as e:
            print(f"Error generating image for {product['name']}: {e}")
            return None
    
    def needs_image(self, product: Dict[str, Any]) -> bool:
        """Check if a product needs an image generated."""
        return 'image_path' not in product or not product.get('image_path')
    
    def process_products(self, limit: Optional[int] = None, delay: float = 1.0):
        """
        Process all products and generate images where needed.
        
        Args:
            limit: Maximum number of images to generate (None for no limit)
            delay: Delay between API calls in seconds
        """
        generated_count = 0
        total_products = 0
        products_needing_images = 0
        
        print("Starting image generation process...")
        print(f"Images will be saved to: {self.images_dir.absolute()}")
        
        # Count total products and those needing images
        for category_name, category_data in self.product_data['main_categories'].items():
            for subcategory_name, products in category_data.items():
                if isinstance(products, list) and products:
                    for product in products:
                        if isinstance(product, dict) and 'name' in product:
                            total_products += 1
                            if self.needs_image(product):
                                products_needing_images += 1
        
        print(f"Total products: {total_products}")
        print(f"Products needing images: {products_needing_images}")
        
        if limit:
            print(f"Generation limit: {limit} images")
        
        # Process each category and subcategory
        for category_name, category_data in self.product_data['main_categories'].items():
            print(f"\nProcessing category: {category_name}")
            
            for subcategory_name, products in category_data.items():
                # Skip non-product data (like seasonal multipliers)
                if not isinstance(products, list) or not products:
                    continue
                
                print(f"  Processing subcategory: {subcategory_name}")
                
                for i, product in enumerate(products):
                    # Skip if not a valid product
                    if not isinstance(product, dict) or 'name' not in product:
                        continue
                    
                    # Check if limit reached
                    if limit and generated_count >= limit:
                        print(f"Reached generation limit of {limit} images")
                        self.save_product_data()
                        return
                    
                    # Check if product needs an image
                    if not self.needs_image(product):
                        print(f"    Skipping {product['name']} (already has image)")
                        continue
                    
                    # Generate image
                    print(f"    Generating image for: {product['name']}")
                    image_path = self.generate_image(product, category_name, subcategory_name)
                    
                    if image_path:
                        # Update product with image path
                        product['image_path'] = image_path
                        generated_count += 1
                        print(f"    ✓ Generated image {generated_count}: {image_path}")
                        
                        # Save progress after each image
                        self.save_product_data()
                        
                        # Add delay to avoid rate limiting
                        if delay > 0:
                            time.sleep(delay)
                    else:
                        print(f"    ✗ Failed to generate image for: {product['name']}")
                        time.sleep(delay)
        
        print("\n🎉 Image generation complete!")
        print(f"Generated {generated_count} new images")
        print(f"All images saved to: {self.images_dir.absolute()}")

    def get_statistics(self) -> Dict[str, int]:
        """Get statistics about products and images."""
        stats = {
            'total_products': 0,
            'products_with_images': 0,
            'products_without_images': 0
        }
        
        for category_name, category_data in self.product_data['main_categories'].items():
            for subcategory_name, products in category_data.items():
                if isinstance(products, list) and products:
                    for product in products:
                        if isinstance(product, dict) and 'name' in product:
                            stats['total_products'] += 1
                            if 'image_path' in product and product.get('image_path'):
                                stats['products_with_images'] += 1
                            else:
                                stats['products_without_images'] += 1
        
        return stats

def main():
    """Main function to run the image generation process."""
    try:
        generator = DalleImageGenerator()
        
        # Show initial statistics
        stats = generator.get_statistics()
        print("📊 Initial Statistics:")
        print(f"  Total products: {stats['total_products']}")
        print(f"  Products with images: {stats['products_with_images']}")
        print(f"  Products without images: {stats['products_without_images']}")
        
        if stats['products_without_images'] == 0:
            print("\n✅ All products already have images!")
            return
        
        # Ask user for preferences
        print("\n" + "="*50)
        print("DALL-E 3 Image Generation Options:")
        print("="*50)
        
        # Get user input for generation limit
        try:
            limit_input = input(f"Enter max images to generate (Enter for all {stats['products_without_images']}): ").strip()
            limit = int(limit_input) if limit_input else None
        except ValueError:
            limit = None
        
        # Get user input for delay
        try:
            delay_input = input("Enter delay between API calls in seconds (default 1.0): ").strip()
            delay = float(delay_input) if delay_input else 1.0
        except ValueError:
            delay = 1.0
        
        print("\nStarting generation with:")
        print(f"  Limit: {limit if limit else 'No limit'}")
        print(f"  Delay: {delay} seconds")
        print(f"  Rate: ~{3600/delay:.0f} images per hour" if delay > 0 else "  Rate: Maximum")
        
        confirm = input("\nProceed? (y/N): ").strip().lower()
        if confirm != 'y':
            print("Generation cancelled.")
            return
        
        # Start generation
        generator.process_products(limit=limit, delay=delay)
        
        # Show final statistics
        final_stats = generator.get_statistics()
        print("\n📊 Final Statistics:")
        print(f"  Total products: {final_stats['total_products']}")
        print(f"  Products with images: {final_stats['products_with_images']}")
        print(f"  Products without images: {final_stats['products_without_images']}")
        
    except KeyboardInterrupt:
        print("\n\n⚠️  Generation interrupted by user")
        print("Progress has been saved automatically.")
    except Exception as e:
        print(f"\n❌ Error: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())
