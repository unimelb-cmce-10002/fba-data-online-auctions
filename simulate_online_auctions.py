import json
import random
from datetime import datetime, timedelta
from faker import Faker
import os

# Initialize Faker
fake = Faker()

# Parameters
NUM_AUCTIONS = 1000
MAX_BIDS_PER_AUCTION = 25

# Category and realistic item titles
CATEGORY_ITEMS = {
    "Electronics": [
        "Apple iPhone 13 Pro", "Samsung Galaxy S22", "Sony WH-1000XM4",
        "GoPro Hero 10", "Dell XPS 13 Laptop", "Apple Watch Series 8"
    ],
    "Books": [
        "Atomic Habits", "Harry Potter and the Philosopher's Stone (1st Ed.)",
        "The Lean Startup", "Sapiens: A Brief History of Humankind",
        "To Kill a Mockingbird", "The Great Gatsby"
    ],
    "Home & Garden": [
        "Dyson V11 Vacuum Cleaner", "IKEA LINNMON Desk",
        "Weber Spirit II Gas Grill", "Philips Hue Starter Kit",
        "Nespresso Vertuo Coffee Machine", "Bosch Cordless Drill"
    ],
    "Fashion": [
        "Nike Air Force 1", "Adidas Ultraboost", "Zara Wool Coat",
        "Louis Vuitton Neverfull Bag", "Casio G-Shock Watch", "Levi's 501 Jeans"
    ],
    "Toys & Games": [
        "LEGO Star Wars Millennium Falcon", "Nintendo Switch",
        "Barbie Dreamhouse", "Hot Wheels Mega Garage",
        "Uno Card Game", "Monopoly Classic Edition"
    ],
    "Collectibles": [
        "Michael Jordan Rookie Card", "Vintage Concert Poster",
        "1980s Comic Book Set", "Rare Pokémon Charizard Card",
        "Antique Pocket Watch", "Funko Pop Marvel Collection"
    ]
}

# Generate a matching title and category
def generate_title_and_category():
    category = random.choices(
        population=list(CATEGORY_ITEMS.keys()),
        weights=[0.25, 0.15, 0.2, 0.15, 0.15, 0.1],
        k=1
    )[0]
    title_base = random.choice(CATEGORY_ITEMS[category])
    suffix = random.choice(["(Like New)", "(Used)", "(2022 Model)", "(Rare)", ""])
    title = f"{title_base} {suffix}".strip()
    return category, title

# Generate a single auction record
def generate_auction(auction_id):
    start_time = fake.date_time_this_year()
    end_time = start_time + timedelta(days=random.randint(1, 3))

    seller = {
        "user_id": fake.user_name(),
        "rating": round(random.uniform(3.5, 5.0), 2),
        "country": fake.country_code()
    }

    num_bids = random.randint(2, MAX_BIDS_PER_AUCTION)
    bid_times = sorted([start_time + timedelta(minutes=random.randint(1, 60 * 24)) for _ in range(num_bids)])
    base_price = random.uniform(10, 100)
    bids = [{
        "bidder_id": fake.user_name(),
        "amount": round(base_price + i * random.uniform(1, 10), 2),
        "time": bid_time.isoformat()
    } for i, bid_time in enumerate(bid_times)]

    category, title = generate_title_and_category()

    return {
        "item_id": auction_id,
        "title": title,
        "category": category,
        "start_time": start_time.isoformat(),
        "end_time": end_time.isoformat(),
        "seller": seller,
        "bids": bids,
        "final_price": bids[-1]["amount"],
        "winner_id": bids[-1]["bidder_id"],
        "tags": fake.words(nb=random.randint(2, 5))
    }

# Main function
if __name__ == "__main__":
    auctions = [generate_auction(f"AUC{str(i).zfill(5)}") for i in range(NUM_AUCTIONS)]

    os.makedirs("output", exist_ok=True)
    ndjson_path = os.path.join("output", "ebay_auctions_large.ndjson")
    json_path = os.path.join("output", "ebay_auctions_large.json")

    # Write NDJSON
    with open(ndjson_path, "w") as f:
        for auction in auctions:
            f.write(json.dumps(auction) + "\n")

    # Write JSON array
    with open(json_path, "w") as f:
        json.dump(auctions, f, indent=2)

    print(f"✅ NDJSON saved to: {ndjson_path}")
    print(f"✅ JSON array saved to: {json_path}")
