import json
import uuid
import random
import requests
from datetime import datetime, timedelta
import os

# -----------------------------
# CONFIG
# -----------------------------

BASE_DAILY_ORDERS = 200
RETURN_RATE = 0.05
ELECTRONICS_RETURN_RATE = 0.08

CURRENCY_DISTRIBUTION = {
    "USD": 0.7,
    "EUR": 0.2,
    "LKR": 0.1
}

PROMOTION_WINDOWS = {
    "black_friday": {"month": 11, "day": 25, "extra_discount": 20},
    "christmas": {"month": 12, "day": 25, "extra_discount": 15}
}

OUTPUT_DIR = "output"
os.makedirs(OUTPUT_DIR, exist_ok=True)

DUMMY_BASE_URL = "https://dummyjson.com"

# -----------------------------
# API EXTRACTION
# -----------------------------

def fetch_all_products():
    print("Fetching products from DummyJSON...")
    response = requests.get(f"{DUMMY_BASE_URL}/products?limit=100")
    response.raise_for_status()
    data = response.json()
    return data["products"]


def fetch_all_users():
    print("Fetching users from DummyJSON...")
    response = requests.get(f"{DUMMY_BASE_URL}/users?limit=100")
    response.raise_for_status()
    data = response.json()
    return data["users"]


def get_exchange_rates():
    try:
        response = requests.get("https://api.exchangerate.host/latest?base=USD")
        data = response.json()
        return {
            "USD": 1.0,
            "EUR": data["rates"]["EUR"],
            "LKR": data["rates"]["LKR"]
        }
    except Exception:
        print("⚠ Exchange API failed. Using fallback rates.")
        return {
            "USD": 1.0,
            "EUR": 0.92,
            "LKR": 310.0
        }


# -----------------------------
# LOGIC HELPERS
# -----------------------------

def weighted_choice(distribution_dict):
    rand = random.random()
    cumulative = 0.0
    for key, weight in distribution_dict.items():
        cumulative += weight
        if rand <= cumulative:
            return key
    return key


def get_daily_order_volume(date):
    base = BASE_DAILY_ORDERS

    if date.weekday() in [5, 6]:
        base *= 1.3

    if date.day >= 25:
        base *= 1.2

    if date.month == 11 and date.day == 25:
        base *= 3

    return int(base)


def assign_customer_segments(users):
    segments = {}
    for user in users:
        r = random.random()
        if r < 0.2:
            segments[user["id"]] = "high"
        elif r < 0.7:
            segments[user["id"]] = "medium"
        else:
            segments[user["id"]] = "low"
    return segments


def customer_probability(segment):
    return {
        "high": 0.7,
        "medium": 0.4,
        "low": 0.15
    }[segment]


def build_product_weights(products):
    weights = {}
    sorted_products = sorted(products, key=lambda x: x["price"], reverse=True)
    top_20 = int(len(products) * 0.2)

    for i, product in enumerate(sorted_products):
        if i < top_20:
            weights[product["id"]] = 0.6 / top_20
        else:
            weights[product["id"]] = 0.4 / (len(products) - top_20)

    return weights


# -----------------------------
# SALES GENERATOR
# -----------------------------

def generate_sales_for_day(target_date, products, users):
    exchange_rates = get_exchange_rates()
    product_weights = build_product_weights(products)
    user_segments = assign_customer_segments(users)

    num_orders = get_daily_order_volume(target_date)

    orders = []
    order_items = []

    for _ in range(num_orders):
        user = random.choice(users)
        segment = user_segments[user["id"]]

        if random.random() > customer_probability(segment):
            continue

        order_id = str(uuid.uuid4())
        currency = weighted_choice(CURRENCY_DISTRIBUTION)

        num_items = random.randint(1, 5)
        total_local = 0
        total_discount = 0

        for _ in range(num_items):
            product_id = weighted_choice(product_weights)
            product = next(p for p in products if p["id"] == product_id)

            quantity = random.randint(1, 3)
            base_price = product["price"]
            discount_pct = random.choice([0, 5, 10, 15])

            # Promotion logic
            for promo in PROMOTION_WINDOWS.values():
                if target_date.month == promo["month"] and target_date.day == promo["day"]:
                    discount_pct += promo["extra_discount"]

            final_price = base_price * (1 - discount_pct / 100) * quantity
            total_local += final_price
            total_discount += base_price * quantity - final_price

            order_items.append({
                "order_item_id": str(uuid.uuid4()),
                "order_id": order_id,
                "product_id": product_id,
                "quantity": quantity,
                "unit_price": base_price,
                "discount_pct": discount_pct,
                "final_price": round(final_price, 2),
                "order_date": target_date.isoformat()
            })

        rate = exchange_rates[currency]
        total_usd = total_local / rate

        # Return logic
        return_prob = RETURN_RATE
        if product["category"].lower() == "electronics":
            return_prob = ELECTRONICS_RETURN_RATE

        order_status = "returned" if random.random() < return_prob else "completed"

        orders.append({
            "order_id": order_id,
            "user_id": user["id"],
            "order_date": target_date.isoformat(),
            "currency": currency,
            "total_amount_local": round(total_local, 2),
            "total_amount_usd": round(total_usd, 2),
            "discount_amount": round(total_discount, 2),
            "payment_method": random.choice(["card", "paypal", "cash"]),
            "order_status": order_status
        })

    return orders, order_items


# -----------------------------
# MAIN
# -----------------------------

if __name__ == "__main__":

    print("Starting Retail Data Pipeline...")

    products = fetch_all_products()
    users = fetch_all_users()

    target_date = datetime.today() - timedelta(days=1)

    orders, order_items = generate_sales_for_day(
        target_date, products, users
    )

    with open(f"{OUTPUT_DIR}/orders_{target_date.date()}.json", "w") as f:
        json.dump(orders, f, indent=2)

    with open(f"{OUTPUT_DIR}/order_items_{target_date.date()}.json", "w") as f:
        json.dump(order_items, f, indent=2)

    print(f"✅ Generated {len(orders)} orders")
    print(f"✅ Generated {len(order_items)} order items")
    print("Retail pipeline completed successfully.")