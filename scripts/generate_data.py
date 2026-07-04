"""
RetailPulse India — Synthetic Data Generator
=============================================
Generates all source data for the Power BI analytics project.
5 years: 2020-2024 | ~1.35M sales rows | 5 heterogeneous sources

Output:
  sources/parquet/sales/year=YYYY/month=MM/sales.parquet
  sources/parquet/returns/year=YYYY/month=MM/returns.parquet
  sources/postgres/dim_customer.sql
  sources/postgres/dim_salesrep.sql
  sources/csv/dim_product.csv
  sources/csv/dim_store.csv
  sources/csv/dim_promotion.csv
  sources/json/india_enrichment.json
  sources/excel/finance_targets.xlsx
"""

import pandas as pd
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import json
import os
from datetime import date, timedelta, datetime
from pathlib import Path

# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────
SEED = 42
rng = np.random.default_rng(SEED)

BASE_DIR = Path(__file__).parent.parent
SOURCES  = BASE_DIR / "sources"

DATE_START = date(2020, 1, 1)
DATE_END   = date(2024, 12, 31)

TARGET_ROWS_PER_YEAR = {
    2020: 180_000,
    2021: 220_000,
    2022: 280_000,
    2023: 320_000,
    2024: 350_000,
}

N_CUSTOMERS   = 80_000
N_PRODUCTS    = 2_500
N_STORES      = 250
N_SALES_REPS  = 800
N_PROMOTIONS  = 150

print("=" * 60)
print("  RetailPulse India — Data Generator")
print("=" * 60)

# ─────────────────────────────────────────────
# INDIAN CALENDAR ENRICHMENT DATA
# ─────────────────────────────────────────────

FESTIVE_PERIODS = [
    # (name, month, day_start, day_end, spike_multiplier, categories_boosted)
    ("Pongal",            1,  13, 16, 1.4,  ["Apparel","Home & Kitchen","Grocery & Food"]),
    ("Republic Day Sale", 1,  24, 26, 1.3,  ["Electronics","Apparel"]),
    ("Holi",              3,  20, 25, 1.5,  ["Home & Kitchen","Beauty & Personal Care","Apparel"]),
    ("Akshaya Tritiya",   4,  20, 25, 1.35, ["Apparel","Home & Kitchen"]),
    ("Independence Day",  8,  12, 16, 1.6,  ["Electronics","Apparel","Sports & Fitness"]),
    ("Onam",              8,  28, 35, 1.7,  ["Apparel","Home & Kitchen","Grocery & Food","Electronics"]),
    ("Navratri",          9,  25, 35, 1.8,  ["Apparel","Beauty & Personal Care","Home & Kitchen"]),
    ("Durga Puja",        9,  30, 38, 1.6,  ["Apparel","Electronics","Home & Kitchen"]),
    ("Dussehra",          10,  1,  5, 1.5,  ["Electronics","Apparel","Books & Stationery"]),
    ("Dhanteras",         10, 20, 25, 2.1,  ["Electronics","Home & Kitchen"]),
    ("Diwali",            10, 24, 30, 2.8,  ["Electronics","Apparel","Home & Kitchen","Beauty & Personal Care","Grocery & Food"]),
    ("Christmas",         12, 22, 27, 1.4,  ["Electronics","Books & Stationery","Apparel"]),
    ("Year End Sale",     12, 28, 31, 1.5,  ["Electronics","Apparel","Sports & Fitness"]),
]

COVID_PERIODS = [
    {"name": "National Lockdown 1.0", "start": "2020-03-25", "end": "2020-06-07",
     "volume_multiplier": 0.35, "online_shift": 0.85},
    {"name": "Unlock Phase 1",        "start": "2020-06-08", "end": "2020-08-31",
     "volume_multiplier": 0.65, "online_shift": 0.70},
    {"name": "Second Wave",           "start": "2021-04-01", "end": "2021-05-31",
     "volume_multiplier": 0.55, "online_shift": 0.75},
    {"name": "Third Wave (Omicron)",  "start": "2022-01-01", "end": "2022-02-15",
     "volume_multiplier": 0.78, "online_shift": 0.60},
]

GST_EVENTS = [
    {"date": "2020-04-01", "description": "GST rate reduction on essential goods"},
    {"date": "2021-06-01", "description": "GST revision on electronics accessories"},
    {"date": "2022-01-01", "description": "GST rate changes on textiles (12%)"},
    {"date": "2023-07-01", "description": "GST on online gaming & hospitality revised"},
    {"date": "2024-02-01", "description": "Union Budget — import duty on electronics revised"},
]

STATE_GDP_INDEX = {
    "Maharashtra":   {"2020": 88, "2021": 79, "2022": 96, "2023": 104, "2024": 109},
    "Delhi":         {"2020": 85, "2021": 78, "2022": 97, "2023": 106, "2024": 111},
    "Karnataka":     {"2020": 87, "2021": 81, "2022": 98, "2023": 107, "2024": 112},
    "Telangana":     {"2020": 89, "2021": 82, "2022": 97, "2023": 105, "2024": 110},
    "Tamil Nadu":    {"2020": 86, "2021": 80, "2022": 96, "2023": 104, "2024": 109},
    "West Bengal":   {"2020": 84, "2021": 77, "2022": 94, "2023": 102, "2024": 106},
    "Gujarat":       {"2020": 88, "2021": 81, "2022": 97, "2023": 105, "2024": 110},
    "Rajasthan":     {"2020": 83, "2021": 76, "2022": 93, "2023": 101, "2024": 105},
    "Uttar Pradesh": {"2020": 82, "2021": 75, "2022": 92, "2023": 100, "2024": 104},
    "Kerala":        {"2020": 85, "2021": 79, "2022": 95, "2023": 103, "2024": 108},
    "Madhya Pradesh":{"2020": 82, "2021": 75, "2022": 92, "2023": 100, "2024": 104},
    "Punjab":        {"2020": 83, "2021": 76, "2022": 93, "2023": 101, "2024": 105},
    "Odisha":        {"2020": 81, "2021": 74, "2022": 91, "2023": 99,  "2024": 103},
    "Jharkhand":     {"2020": 80, "2021": 73, "2022": 90, "2023": 98,  "2024": 102},
    "Chhattisgarh":  {"2020": 80, "2021": 73, "2022": 90, "2023": 98,  "2024": 102},
}

# ─────────────────────────────────────────────
# TAXONOMY
# ─────────────────────────────────────────────

CATEGORY_TAXONOMY = {
    "Electronics": {
        "margin_range": (0.08, 0.14),
        "return_rate":  (0.08, 0.12),
        "aov_range":    (3500, 45000),
        "subcategories": {
            "Large Appliances":    ["Refrigerator","Washing Machine","Air Conditioner","Microwave Oven","Dishwasher"],
            "Small Appliances":    ["Mixer Grinder","Juicer","Electric Kettle","Air Fryer","Induction Cooktop","Vacuum Cleaner","Ceiling Fan"],
            "Consumer Electronics":["Smartphone","Tablet","Laptop","Smart TV","Monitor","Camera","Headphones","Earbuds","Bluetooth Speaker"],
            "IT Accessories":      ["Keyboard","Mouse","USB Hub","Pen Drive","External Hard Drive","Webcam","Laptop Bag","Screen Guard","Power Bank"],
            "Wearables":           ["Smartwatch","Fitness Band","Wireless Earphones","VR Headset"],
        }
    },
    "Apparel": {
        "margin_range": (0.38, 0.52),
        "return_rate":  (0.06, 0.09),
        "aov_range":    (400, 8500),
        "subcategories": {
            "Men's Clothing":   ["Formal Shirt","Casual T-Shirt","Trousers","Jeans","Kurta","Jacket","Suit & Blazer","Shorts"],
            "Women's Clothing": ["Saree","Salwar Kameez","Kurti","Western Top","Jeans","Dress","Leggings","Ethnic Set"],
            "Kids' Clothing":   ["Boys Wear","Girls Wear","Infant Clothing","School Uniform","Kids Ethnic Wear"],
            "Footwear":         ["Men's Formal Shoes","Men's Casual Shoes","Women's Heels","Women's Flats","Sports Shoes","Sandals","Slippers"],
            "Accessories":      ["Handbag","Wallet","Belt","Sunglasses","Fashion Watch","Scarf","Cap"],
        }
    },
    "Home & Kitchen": {
        "margin_range": (0.28, 0.36),
        "return_rate":  (0.03, 0.05),
        "aov_range":    (300, 15000),
        "subcategories": {
            "Cookware":          ["Pressure Cooker","Kadai","Tawa","Non-stick Pan","Steel Utensil Set","Casserole"],
            "Kitchen Storage":   ["Container Set","Water Bottle","Lunch Box","Rack System","Kitchen Organizer"],
            "Home Decor":        ["Curtain Set","Cushion Cover","Bedsheet Set","Pillow Cover","Wall Art","Diya Set","Festive Decor"],
            "Furniture":         ["Study Table","Chair","Bookshelf","Wardrobe","Bed Frame","Sofa","Dining Table"],
            "Cleaning & Utility":["Mop & Bucket","Broom Set","Dustbin","Storage Box","Hanger Set","Laundry Basket"],
        }
    },
    "Beauty & Personal Care": {
        "margin_range": (0.52, 0.68),
        "return_rate":  (0.02, 0.04),
        "aov_range":    (150, 4500),
        "subcategories": {
            "Skincare":       ["Moisturizer","Sunscreen SPF50","Face Serum","Face Wash","Toner","Face Mask","Eye Cream"],
            "Haircare":       ["Shampoo","Conditioner","Hair Oil","Hair Serum","Dry Shampoo","Hair Mask"],
            "Makeup":         ["Foundation","Lipstick","Kajal","Eyeshadow Palette","Blush","Nail Polish Set","Compact Powder"],
            "Men's Grooming": ["Shaving Kit","Beard Oil","Men's Face Wash","Trimmer","After Shave"],
            "Fragrances":     ["Eau de Parfum","Deodorant","Body Mist","Attar","Gift Set"],
        }
    },
    "Sports & Fitness": {
        "margin_range": (0.31, 0.44),
        "return_rate":  (0.04, 0.07),
        "aov_range":    (300, 12000),
        "subcategories": {
            "Exercise Equipment": ["Dumbbell Set","Resistance Bands","Yoga Mat","Jump Rope","Pull-up Bar","Foam Roller","Kettlebell"],
            "Sports Gear":        ["Cricket Kit","Badminton Set","Football","Carrom Board","Chess Set","Table Tennis Set","Shuttle Cocks"],
            "Cycling":            ["Cycle","Helmet","Cycling Gloves","Cycle Lock","Cycling Water Bottle","Cycle Pump"],
            "Outdoor & Adventure":["Trekking Shoes","Trekking Backpack","Tent","Sleeping Bag","Headlamp","Trekking Poles"],
            "Sports Nutrition":   ["Whey Protein","Energy Bar","BCAA Supplement","Multivitamin","Creatine","Pre-Workout"],
        }
    },
    "Books & Stationery": {
        "margin_range": (0.44, 0.58),
        "return_rate":  (0.005, 0.015),
        "aov_range":    (150, 3500),
        "subcategories": {
            "Academic Books":    ["Engineering Textbook","Medical Reference","MBA Casebook","School Textbook","Competitive Exam Guide","UPSC Material"],
            "Fiction":           ["Indian Fiction","International Bestseller","Regional Novel","Children's Fiction","Graphic Novel"],
            "Non-Fiction":       ["Business Book","Self-Help","Biography","Popular Science","History","Personal Finance"],
            "Stationery":        ["Notebook","Pen Set","Marker Set","Art Supply Kit","Planner","Sticky Notes","File Folder Set"],
            "Digital Learning":  ["Online Course Voucher","Coding Kit for Kids","Learning Subscription Card","STEM Kit"],
        }
    },
    "Grocery & Food": {
        "margin_range": (0.04, 0.09),
        "return_rate":  (0.003, 0.008),
        "aov_range":    (200, 5000),
        "subcategories": {
            "Staples":          ["Basmati Rice","Wheat Atta","Toor Dal","Sugar","Cooking Oil","Rock Salt","Chana Dal"],
            "Packaged Foods":   ["Biscuits Pack","Chips & Snacks","Instant Noodles","Ready-to-Eat Meal","Breakfast Cereal","Popcorn"],
            "Beverages":        ["Premium Tea","Filter Coffee","Packaged Juice","Health Drink","Sparkling Water","Coconut Water"],
            "Dairy & Eggs":     ["Full Cream Milk","Cheese Block","Butter","Greek Yoghurt","Paneer","Eggs (Tray)"],
            "Organic & Health": ["Organic Dal","Cold-Pressed Oil","Chia Seeds","Quinoa","Herbal Tea","Mixed Dry Fruits","Honey"],
        }
    },
}

TIER1_CITIES = {
    "Mumbai":    "Maharashtra",
    "Delhi":     "Delhi",
    "Bangalore": "Karnataka",
    "Hyderabad": "Telangana",
    "Chennai":   "Tamil Nadu",
    "Kolkata":   "West Bengal",
    "Pune":      "Maharashtra",
    "Ahmedabad": "Gujarat",
}

TIER2_CITIES = {
    "Jaipur":         "Rajasthan",
    "Lucknow":        "Uttar Pradesh",
    "Surat":          "Gujarat",
    "Kochi":          "Kerala",
    "Nagpur":         "Maharashtra",
    "Indore":         "Madhya Pradesh",
    "Bhopal":         "Madhya Pradesh",
    "Visakhapatnam":  "Telangana",
    "Patna":          "Bihar",
    "Vadodara":       "Gujarat",
    "Ludhiana":       "Punjab",
    "Coimbatore":     "Tamil Nadu",
    "Madurai":        "Tamil Nadu",
    "Nashik":         "Maharashtra",
    "Mysore":         "Karnataka",
    "Ranchi":         "Jharkhand",
    "Raipur":         "Chhattisgarh",
    "Bhubaneswar":    "Odisha",
}

TIER3_CITIES = {
    "Agra":         "Uttar Pradesh",
    "Varanasi":     "Uttar Pradesh",
    "Meerut":       "Uttar Pradesh",
    "Amritsar":     "Punjab",
    "Jodhpur":      "Rajasthan",
    "Udaipur":      "Rajasthan",
    "Guwahati":     "Assam",
    "Mangalore":    "Karnataka",
    "Hubli":        "Karnataka",
    "Tiruchirappalli": "Tamil Nadu",
    "Tirupati":     "Telangana",
    "Vijayawada":   "Telangana",
    "Warangal":     "Telangana",
    "Rajkot":       "Gujarat",
    "Jabalpur":     "Madhya Pradesh",
    "Gwalior":      "Madhya Pradesh",
    "Dhanbad":      "Jharkhand",
    "Jamshedpur":   "Jharkhand",
    "Bareilly":     "Uttar Pradesh",
    "Aligarh":      "Uttar Pradesh",
    "Moradabad":    "Uttar Pradesh",
    "Saharanpur":   "Uttar Pradesh",
    "Gorakhpur":    "Uttar Pradesh",
    "Bikaner":      "Rajasthan",
}

STORE_TYPES   = ["Flagship Store","Express Store","Online Only","Shop-in-Shop"]
STORE_TYPE_W  = [0.20, 0.35, 0.30, 0.15]

PAYMENT_METHODS = ["UPI","Credit Card","Debit Card","Net Banking","Cash on Delivery","EMI","Wallet"]
PAYMENT_W       = [0.38, 0.18, 0.14, 0.08, 0.12, 0.07, 0.03]

ACQUISITION_CHANNELS = ["Organic Search","Paid Ads","Social Media","Referral","Email Campaign","App Store","Walk-in","Word of Mouth"]
LOYALTY_TIERS        = ["Bronze","Silver","Gold","Platinum"]
LOYALTY_W            = [0.50, 0.28, 0.15, 0.07]

AGE_GROUPS = ["18-24","25-34","35-44","45-54","55-64","65+"]
AGE_W      = [0.18, 0.32, 0.25, 0.14, 0.07, 0.04]

GENDERS = ["Male","Female","Non-Binary","Prefer Not to Say"]
GENDER_W = [0.48, 0.46, 0.03, 0.03]

PROMO_TYPES   = ["Percentage Discount","Flat Discount","Buy X Get Y","Bundle Offer","Clearance Sale","Seasonal Sale","Flash Sale","Loyalty Reward"]
PROMO_CHANNELS= ["Email","SMS","App Notification","Social Media","In-Store","Affiliate"]

RETURN_REASONS = [
    "Product Damaged in Transit",
    "Wrong Product Delivered",
    "Product Not as Described",
    "Quality Not Satisfactory",
    "Size/Fit Issue",
    "Changed Mind",
    "Found Better Price Elsewhere",
    "Duplicate Order",
    "Product Stopped Working",
]

EXPERIENCE_LEVELS = ["Junior","Mid-Level","Senior","Team Lead"]

# ─────────────────────────────────────────────
# HELPER UTILITIES
# ─────────────────────────────────────────────

def weighted_choice(choices, weights, size=1):
    weights = np.array(weights, dtype=float)
    weights /= weights.sum()
    idx = rng.choice(len(choices), size=size, p=weights)
    return [choices[i] for i in idx] if size > 1 else choices[idx[0]]

def date_range_list(start: date, end: date):
    delta = (end - start).days + 1
    return [start + timedelta(days=i) for i in range(delta)]

all_dates = date_range_list(DATE_START, DATE_END)

def build_covid_lookup():
    lookup = {}
    for period in COVID_PERIODS:
        s = date.fromisoformat(period["start"])
        e = date.fromisoformat(period["end"])
        for d in date_range_list(s, e):
            lookup[d] = period
    return lookup

covid_lookup = build_covid_lookup()

def get_festive_multiplier(d: date, category: str):
    """Return (multiplier, festive_name) for a given date and category."""
    for name, month, day_s, day_e, mult, cats in FESTIVE_PERIODS:
        # Diwali/Navratri shift ±7 days per year based on lunar calendar
        shift = rng.integers(-5, 6)
        start_day = day_s + shift
        end_day   = day_e + shift
        if d.month == month and start_day <= d.day <= end_day:
            if category in cats:
                return mult, name
    return 1.0, None

# ─────────────────────────────────────────────
# 1. DIM_DATE → JSON enrichment
# ─────────────────────────────────────────────

print("\n[1/9] Building date dimension & JSON enrichment...")

# Build Indian public holidays (fixed + approximate)
PUBLIC_HOLIDAYS = set()
for yr in range(2020, 2025):
    fixed = [
        date(yr, 1, 26),   # Republic Day
        date(yr, 8, 15),   # Independence Day
        date(yr, 10, 2),   # Gandhi Jayanti
        date(yr, 12, 25),  # Christmas
    ]
    PUBLIC_HOLIDAYS.update(fixed)

date_records = []
for d in all_dates:
    covid = covid_lookup.get(d)
    is_covid     = covid is not None
    covid_name   = covid["name"] if covid else None
    vol_mult     = covid["volume_multiplier"] if covid else 1.0
    online_shift = covid["online_shift"] if covid else 0.0

    week_num = d.isocalendar()[1]
    quarter  = (d.month - 1) // 3 + 1
    fiscal_quarter = ((d.month + 2) % 12) // 3 + 1  # Apr-Mar fiscal year
    fiscal_year    = d.year if d.month >= 4 else d.year - 1

    date_records.append({
        "DateID":          d.isoformat(),
        "Date":            d.isoformat(),
        "Year":            d.year,
        "Month":           d.month,
        "MonthName":       d.strftime("%B"),
        "Quarter":         quarter,
        "QuarterLabel":    f"Q{quarter}",
        "FiscalYear":      fiscal_year,
        "FiscalQuarter":   fiscal_quarter,
        "WeekNumber":      week_num,
        "DayOfWeek":       d.weekday(),
        "DayName":         d.strftime("%A"),
        "IsWeekend":       d.weekday() >= 5,
        "IsPublicHoliday": d in PUBLIC_HOLIDAYS,
        "IsCovidPeriod":   is_covid,
        "CovidPeriodName": covid_name,
        "VolumeMultiplier":vol_mult,
        "OnlineShiftRate": online_shift,
        "YearMonth":       d.strftime("%Y-%m"),
    })

enrichment = {
    "metadata": {
        "generated_at": datetime.now().isoformat(),
        "description":  "India retail calendar enrichment — festive periods, COVID, GST events, state GDP index",
        "source":       "Synthetic — RetailPulse Data Engineering Team",
        "coverage":     "2020-01-01 to 2024-12-31"
    },
    "festive_periods": [
        {"name": f[0], "typical_month": f[1], "typical_day_start": f[2],
         "typical_day_end": f[3], "peak_multiplier": f[4],
         "boosted_categories": f[5]} for f in FESTIVE_PERIODS
    ],
    "covid_periods":   COVID_PERIODS,
    "gst_events":      GST_EVENTS,
    "state_gdp_index": STATE_GDP_INDEX,
    "public_holidays": [d.isoformat() for d in sorted(PUBLIC_HOLIDAYS)],
    "date_dimension":  date_records
}

json_path = SOURCES / "json" / "india_enrichment.json"
with open(json_path, "w", encoding="utf-8") as f:
    json.dump(enrichment, f, indent=2, ensure_ascii=False)
print(f"    ✓ india_enrichment.json — {len(date_records):,} date rows")

# ─────────────────────────────────────────────
# 2. DIM_PRODUCT → CSV
# ─────────────────────────────────────────────

print("\n[2/9] Building dim_product...")

products = []
pid = 1
for cat, cfg in CATEGORY_TAXONOMY.items():
    for subcat, items in cfg["subcategories"].items():
        per_item = max(2, N_PRODUCTS // sum(
            len(v) for c in CATEGORY_TAXONOMY.values()
            for v in c["subcategories"].values()
        ) + 1)
        for item in items:
            for variant in range(per_item):
                lo, hi = cfg["aov_range"]
                cost_price = round(float(rng.uniform(lo * 0.45, hi * 0.55)), 2)
                margin     = float(rng.uniform(*cfg["margin_range"]))
                list_price = round(cost_price / (1 - margin), 2)
                brand_suffix = rng.choice(["Pro","Plus","Elite","Lite","Max","Eco","Classic","Premium","Select"])
                brand = f"Brand{rng.integers(1, 51):02d}"

                products.append({
                    "ProductID":     f"PRD{pid:05d}",
                    "ProductName":   f"{item} {brand_suffix}" if variant > 0 else item,
                    "Category":      cat,
                    "SubCategory":   subcat,
                    "Brand":         brand,
                    "SupplierID":    f"SUP{rng.integers(1, 201):03d}",
                    "CostPrice":     cost_price,
                    "ListPrice":     list_price,
                    "MarginPct":     round(margin * 100, 2),
                    "IsActive":      True if rng.random() > 0.05 else False,
                    "LaunchDate":    date(rng.integers(2018, 2024),
                                         int(rng.integers(1, 13)),
                                         int(rng.integers(1, 28))).isoformat(),
                    "Weight_kg":     round(float(rng.uniform(0.1, 25.0)), 2),
                })
                pid += 1
                if pid > N_PRODUCTS:
                    break
            if pid > N_PRODUCTS:
                break
        if pid > N_PRODUCTS:
            break

# ── Inject DQ issue: casing variants on Category (~80 rows)
dq_indices = rng.choice(len(products), size=80, replace=False)
casing_variants = ["electronics","APPAREL","home & kitchen","BEAUTY & PERSONAL CARE","sports & fitness"]
for i in dq_indices[:40]:
    products[i]["Category"] = products[i]["Category"].lower()
for i in dq_indices[40:]:
    products[i]["Category"] = products[i]["Category"].upper()

df_product = pd.DataFrame(products[:N_PRODUCTS])
df_product.to_csv(SOURCES / "csv" / "dim_product.csv", index=False)
print(f"    ✓ dim_product.csv — {len(df_product):,} rows | {df_product['Category'].nunique()} categories (with casing DQ issues)")

# ─────────────────────────────────────────────
# 3. DIM_STORE → CSV
# ─────────────────────────────────────────────

print("\n[3/9] Building dim_store...")

all_cities = (
    [(c, s, "Tier 1") for c, s in TIER1_CITIES.items()] +
    [(c, s, "Tier 2") for c, s in TIER2_CITIES.items()] +
    [(c, s, "Tier 3") for c, s in TIER3_CITIES.items()]
)

stores = []
sid = 1
for i in range(N_STORES):
    city, state, tier = all_cities[i % len(all_cities)]
    store_type = weighted_choice(STORE_TYPES, STORE_TYPE_W)
    open_year  = int(rng.integers(2012, 2022))
    open_month = int(rng.integers(1, 13))

    # Revenue potential by tier
    rev_potential = {"Tier 1": float(rng.uniform(8, 20)),
                     "Tier 2": float(rng.uniform(4, 10)),
                     "Tier 3": float(rng.uniform(1.5, 5))}[tier]

    # Rent index (inverse of margin)
    rent_index = {"Tier 1": float(rng.uniform(75, 100)),
                  "Tier 2": float(rng.uniform(40, 70)),
                  "Tier 3": float(rng.uniform(15, 38))}[tier]

    stores.append({
        "StoreID":          f"STR{sid:04d}",
        "StoreName":        f"RetailPulse {city} {store_type.split()[0]} {sid}",
        "StoreType":        store_type,
        "City":             city,
        "State":            state,
        "Tier":             tier,
        "Region":           ("West" if state in ["Maharashtra","Gujarat","Goa","Rajasthan"] else
                             "South" if state in ["Karnataka","Tamil Nadu","Telangana","Kerala","Andhra Pradesh"] else
                             "East"  if state in ["West Bengal","Odisha","Jharkhand","Bihar","Assam"] else
                             "North"),
        "OpenDate":         date(open_year, open_month, 1).isoformat(),
        "IsActive":         True if rng.random() > 0.06 else False,
        "FloorAreaSqFt":    int(rng.integers(800, 15000)) if store_type != "Online Only" else 0,
        "MonthlyRentINR":   int(rent_index * rng.uniform(10000, 60000)) if store_type != "Online Only" else 0,
        "RevenuePotentialCr": round(rev_potential, 2),
        "ManagerID":        f"EMP{rng.integers(1000, 9999):04d}",
    })
    sid += 1

df_store = pd.DataFrame(stores)
df_store.to_csv(SOURCES / "csv" / "dim_store.csv", index=False)
print(f"    ✓ dim_store.csv — {len(df_store):,} rows across {df_store['Tier'].nunique()} tiers, {df_store['Region'].nunique()} regions")

# ─────────────────────────────────────────────
# 4. DIM_PROMOTION → CSV
# ─────────────────────────────────────────────

print("\n[4/9] Building dim_promotion...")

promotions = []
for i in range(1, N_PROMOTIONS + 1):
    ptype   = weighted_choice(PROMO_TYPES, [0.25,0.20,0.10,0.10,0.10,0.10,0.10,0.05])
    channel = weighted_choice(PROMO_CHANNELS, [0.25,0.20,0.20,0.15,0.10,0.10])
    disc_band = weighted_choice(["0-10%","10-20%","20-30%","30-50%","50%+"],
                                 [0.20, 0.35, 0.25, 0.15, 0.05])
    disc_val  = {"0-10%": float(rng.uniform(2,10)),
                 "10-20%": float(rng.uniform(10,20)),
                 "20-30%": float(rng.uniform(20,30)),
                 "30-50%": float(rng.uniform(30,50)),
                 "50%+":   float(rng.uniform(50,70))}[disc_band]

    promotions.append({
        "PromotionID":    f"PRM{i:04d}",
        "PromotionName":  f"{ptype} — {channel} {i}",
        "PromotionType":  ptype,
        "Channel":        channel,
        "DiscountBand":   disc_band,
        "DiscountPct":    round(disc_val, 2),
        "StartDate":      date(rng.integers(2020,2025),
                               int(rng.integers(1,13)),
                               int(rng.integers(1,15))).isoformat(),
        "DurationDays":   int(rng.integers(1, 31)),
        "BudgetINRLakhs": round(float(rng.uniform(1, 150)), 2),
        "TargetCategory": weighted_choice(list(CATEGORY_TAXONOMY.keys()),
                                          [1]*len(CATEGORY_TAXONOMY)),
        "IsActive":       True if rng.random() > 0.30 else False,
    })

df_promo = pd.DataFrame(promotions)
df_promo.to_csv(SOURCES / "csv" / "dim_promotion.csv", index=False)
print(f"    ✓ dim_promotion.csv — {len(df_promo):,} rows")

# ─────────────────────────────────────────────
# 5. DIM_CUSTOMER + DIM_SALESREP → PostgreSQL SQL
# ─────────────────────────────────────────────

print("\n[5/9] Building dim_customer & dim_salesrep (PostgreSQL SQL)...")

first_names = ["Aarav","Vivaan","Aditya","Vihaan","Arjun","Sai","Reyansh","Ayaan","Krishna","Ishaan",
               "Priya","Ananya","Diya","Anika","Kavya","Pooja","Shruti","Meera","Riya","Nisha",
               "Rahul","Rohit","Amit","Suresh","Raj","Vijay","Manoj","Deepak","Sandeep","Kiran",
               "Sunita","Rekha","Geeta","Asha","Seema","Usha","Radha","Lakshmi","Savita","Poonam"]

last_names  = ["Sharma","Verma","Patel","Shah","Mehta","Gupta","Kumar","Singh","Jain","Reddy",
               "Nair","Iyer","Pillai","Menon","Krishnan","Rao","Agarwal","Saxena","Mishra","Tiwari",
               "Challa","Naidu","Murthy","Raju","Prasad","Gowda","Hegde","Shetty","Kaur","Bhat"]

customers = []
for i in range(1, N_CUSTOMERS + 1):
    city_entry  = all_cities[rng.integers(0, len(all_cities))]
    city, state, tier = city_entry
    region = ("West" if state in ["Maharashtra","Gujarat","Goa","Rajasthan"] else
              "South" if state in ["Karnataka","Tamil Nadu","Telangana","Kerala","Andhra Pradesh"] else
              "East"  if state in ["West Bengal","Odisha","Jharkhand","Bihar","Assam"] else "North")

    fname = rng.choice(first_names)
    lname = rng.choice(last_names)

    # DQ: 4.2% null AgeGroup, 2.8% null Region
    age_grp = weighted_choice(AGE_GROUPS, AGE_W) if rng.random() > 0.042 else None
    reg_val = region if rng.random() > 0.028 else None

    acq_year  = int(rng.integers(2019, 2025))
    acq_month = int(rng.integers(1, 13))

    customers.append({
        "CustomerID":         f"CUS{i:06d}",
        "FirstName":          fname,
        "LastName":           lname,
        "Email":              f"{fname.lower()}.{lname.lower()}{i}@example.com",
        "Gender":             weighted_choice(GENDERS, GENDER_W),
        "AgeGroup":           age_grp,
        "City":               city,
        "State":              state,
        "Region":             reg_val,
        "Tier":               tier,
        "LoyaltyTier":        weighted_choice(LOYALTY_TIERS, LOYALTY_W),
        "AcquisitionChannel": weighted_choice(ACQUISITION_CHANNELS, [0.22,0.20,0.18,0.10,0.12,0.10,0.05,0.03]),
        "AcquisitionDate":    date(acq_year, acq_month, int(rng.integers(1, 28))).isoformat(),
        "IsActive":           True if rng.random() > 0.12 else False,
        "CLV_Score":          round(float(rng.uniform(100, 9500)), 2),
    })

df_customer = pd.DataFrame(customers)

# Write PostgreSQL SQL
def df_to_sql_inserts(df, table_name, chunksize=1000):
    lines = []
    cols  = ", ".join(df.columns)

    def fmt(v):
        if v is None or (isinstance(v, float) and np.isnan(v)):
            return "NULL"
        if isinstance(v, bool):
            return "TRUE" if v else "FALSE"
        if isinstance(v, (int, float)):
            return str(v)
        return "'" + str(v).replace("'", "''") + "'"

    for start in range(0, len(df), chunksize):
        chunk = df.iloc[start:start+chunksize]
        vals  = ",\n  ".join(
            "(" + ", ".join(fmt(r[c]) for c in df.columns) + ")"
            for _, r in chunk.iterrows()
        )
        lines.append(f"INSERT INTO {table_name} ({cols}) VALUES\n  {vals};")
    return "\n\n".join(lines)

cust_ddl = """
-- ============================================================
-- RetailPulse India — CRM Database (PostgreSQL)
-- Table: dim_customer
-- Source: Operational CRM System
-- ============================================================

DROP TABLE IF EXISTS dim_customer;
CREATE TABLE dim_customer (
    CustomerID         VARCHAR(10)   PRIMARY KEY,
    FirstName          VARCHAR(50),
    LastName           VARCHAR(50),
    Email              VARCHAR(120),
    Gender             VARCHAR(30),
    AgeGroup           VARCHAR(10),   -- NULLABLE: 4.2% null (CRM data gap pre-2021)
    City               VARCHAR(60),
    State              VARCHAR(60),
    Region             VARCHAR(20),   -- NULLABLE: 2.8% null (early records)
    Tier               VARCHAR(10),
    LoyaltyTier        VARCHAR(15),
    AcquisitionChannel VARCHAR(40),
    AcquisitionDate    DATE,
    IsActive           BOOLEAN,
    CLV_Score          NUMERIC(10,2)
);

"""

cust_sql = cust_ddl + df_to_sql_inserts(df_customer, "dim_customer")
with open(SOURCES / "postgres" / "dim_customer.sql", "w", encoding="utf-8") as f:
    f.write(cust_sql)

# Sales Reps
reps = []
rep_cities = [all_cities[rng.integers(0, len(all_cities))] for _ in range(N_SALES_REPS)]
for i in range(1, N_SALES_REPS + 1):
    city, state, tier = rep_cities[i-1]
    region = ("West" if state in ["Maharashtra","Gujarat","Goa","Rajasthan"] else
              "South" if state in ["Karnataka","Tamil Nadu","Telangana","Kerala","Andhra Pradesh"] else
              "East"  if state in ["West Bengal","Odisha","Jharkhand","Bihar","Assam"] else "North")
    fname = rng.choice(first_names)
    lname = rng.choice(last_names)
    reps.append({
        "SalesRepID":      f"REP{i:04d}",
        "FirstName":       fname,
        "LastName":        lname,
        "Email":           f"{fname.lower()}.{lname.lower()}.rep{i}@retailpulse.in",
        "Region":          region,
        "City":            city,
        "TeamID":          f"TEAM{rng.integers(1, 41):02d}",
        "ExperienceLevel": weighted_choice(EXPERIENCE_LEVELS, [0.35,0.35,0.20,0.10]),
        "JoinDate":        date(int(rng.integers(2016, 2024)),
                                int(rng.integers(1, 13)),
                                int(rng.integers(1, 28))).isoformat(),
        "IsActive":        True if rng.random() > 0.08 else False,
        "QuotaINRLakhs":   round(float(rng.uniform(20, 200)), 2),
    })

df_rep = pd.DataFrame(reps)
rep_ddl = """
-- ============================================================
-- RetailPulse India — CRM Database (PostgreSQL)
-- Table: dim_salesrep
-- Source: Operational CRM System
-- ============================================================

DROP TABLE IF EXISTS dim_salesrep;
CREATE TABLE dim_salesrep (
    SalesRepID      VARCHAR(8)    PRIMARY KEY,
    FirstName       VARCHAR(50),
    LastName        VARCHAR(50),
    Email           VARCHAR(120),
    Region          VARCHAR(20),
    City            VARCHAR(60),
    TeamID          VARCHAR(8),
    ExperienceLevel VARCHAR(15),
    JoinDate        DATE,
    IsActive        BOOLEAN,
    QuotaINRLakhs   NUMERIC(10,2)
);

"""
rep_sql = rep_ddl + df_to_sql_inserts(df_rep, "dim_salesrep")
with open(SOURCES / "postgres" / "dim_salesrep.sql", "w", encoding="utf-8") as f:
    f.write(rep_sql)

print(f"    ✓ dim_customer.sql — {len(df_customer):,} rows (nulls in AgeGroup: {df_customer['AgeGroup'].isna().sum():,}, Region: {df_customer['Region'].isna().sum():,})")
print(f"    ✓ dim_salesrep.sql — {len(df_rep):,} rows")

# ─────────────────────────────────────────────
# 6. FINANCE TARGETS → Excel
# ─────────────────────────────────────────────

print("\n[6/9] Building finance_targets.xlsx...")

regions   = ["North","South","East","West"]
categories= list(CATEGORY_TAXONOMY.keys())
quarters  = [1, 2, 3, 4]
years     = [2020, 2021, 2022, 2023, 2024]

targets = []
base_targets = {
    "Electronics":         12_00_00_000,
    "Apparel":             8_00_00_000,
    "Home & Kitchen":      6_50_00_000,
    "Beauty & Personal Care": 3_00_00_000,
    "Sports & Fitness":    4_00_00_000,
    "Books & Stationery":  2_00_00_000,
    "Grocery & Food":      5_50_00_000,
}
region_weights = {"North": 0.28, "South": 0.30, "East": 0.18, "West": 0.24}
quarter_weights= {1: 0.22, 2: 0.20, 3: 0.28, 4: 0.30}  # Q4 heaviest (Diwali)
growth_by_year = {2020: 0.82, 2021: 1.00, 2022: 1.18, 2023: 1.32, 2024: 1.45}

for yr in years:
    for qtr in quarters:
        for reg in regions:
            for cat in categories:
                base = base_targets[cat]
                target_rev = (base * region_weights[reg] *
                              quarter_weights[qtr] * growth_by_year[yr] *
                              float(rng.uniform(0.95, 1.08)))  # finance optimism
                margin_tgt = float(rng.uniform(*CATEGORY_TAXONOMY[cat]["margin_range"])) * 0.90
                targets.append({
                    "Year":               yr,
                    "Quarter":            qtr,
                    "QuarterLabel":       f"Q{qtr} {yr}",
                    "Region":             reg,
                    "Category":           cat,
                    "RevenueTargetINR":   round(target_rev, 0),
                    "MarginTargetPct":    round(margin_tgt * 100, 2),
                    "GrowthTargetPct":    round((growth_by_year[yr] - 1) * 100 + float(rng.uniform(-2,5)), 2),
                    "OrderVolumeTarget":  int(target_rev / 2500 * float(rng.uniform(0.9, 1.1))),
                    "Notes":              ("COVID adjustment" if yr == 2020 and qtr in [1,2] else
                                          "Post-COVID recovery" if yr == 2021 else
                                          "Aggressive expansion" if yr == 2023 else ""),
                })

df_targets = pd.DataFrame(targets)

with pd.ExcelWriter(SOURCES / "excel" / "finance_targets.xlsx", engine="openpyxl") as writer:
    df_targets.to_excel(writer, sheet_name="Quarterly Targets", index=False)

    # Summary pivot sheet
    pivot = df_targets.groupby(["Year","Category"])["RevenueTargetINR"].sum().reset_index()
    pivot.to_excel(writer, sheet_name="Annual Summary", index=False)

    # Metadata sheet
    meta = pd.DataFrame([
        {"Field": "RevenueTargetINR", "Description": "Revenue target in Indian Rupees"},
        {"Field": "MarginTargetPct",  "Description": "Net margin % target set by Finance"},
        {"Field": "GrowthTargetPct",  "Description": "YoY growth target"},
        {"Field": "Source",           "Description": "Finance team — manually maintained"},
        {"Field": "Refresh Cadence",  "Description": "Quarterly"},
        {"Field": "Owner",            "Description": "CFO Office"},
        {"Field": "Last Updated",     "Description": datetime.now().strftime("%Y-%m-%d")},
    ])
    meta.to_excel(writer, sheet_name="Metadata", index=False)

print(f"    ✓ finance_targets.xlsx — {len(df_targets):,} rows | 3 sheets")

# ─────────────────────────────────────────────
# 7. FACT_SALES → Partitioned Parquet
# ─────────────────────────────────────────────

print("\n[7/9] Building fact_sales (partitioned Parquet)...")
print("      This is the large table — ~1.35M rows. Takes a few minutes...")

product_ids  = df_product["ProductID"].tolist()
store_ids    = df_store["StoreID"].tolist()
customer_ids = df_customer["CustomerID"].tolist()
rep_ids      = df_rep["SalesRepID"].tolist()
promo_ids    = df_promo["PromotionID"].tolist()

# Pre-build category lookup for products
prod_cat  = dict(zip(df_product["ProductID"], df_product["Category"]))
prod_cost = dict(zip(df_product["ProductID"], df_product["CostPrice"]))
prod_list = dict(zip(df_product["ProductID"], df_product["ListPrice"]))

# Pre-build store region lookup
store_region = dict(zip(df_store["StoreID"], df_store["Region"]))
store_tier   = dict(zip(df_store["StoreID"], df_store["Tier"]))
store_type   = dict(zip(df_store["StoreID"], df_store["StoreType"]))

total_rows_generated = 0
all_order_ids = []

for year, n_rows in TARGET_ROWS_PER_YEAR.items():
    year_dates = [d for d in all_dates if d.year == year]
    print(f"      Generating {year}: {n_rows:,} rows...")

    # Monthly distribution weights (heavier in Oct-Dec for festive)
    month_weights = np.array([0.07,0.07,0.07,0.07,0.07,0.07,0.08,0.09,0.09,0.12,0.12,0.08], dtype=float)
    month_weights /= month_weights.sum()

    records = []
    order_counter = total_rows_generated + 1

    for month in range(1, 13):
        month_dates = [d for d in year_dates if d.month == month]
        if not month_dates:
            continue
        n_month = int(n_rows * month_weights[month - 1])

        for _ in range(n_month):
            d = month_dates[rng.integers(0, len(month_dates))]

            # COVID suppression
            covid = covid_lookup.get(d)
            vol_adj = covid["volume_multiplier"] if covid else 1.0
            if rng.random() > vol_adj:
                continue  # skip this order due to COVID

            # Pick product (category-weighted for day/season)
            cat_weights = np.array([0.22,0.19,0.16,0.11,0.10,0.08,0.14], dtype=float)
            # Boost categories during COVID for WFH
            if covid and year == 2020:
                cat_weights[0] *= 1.4  # Electronics
                cat_weights[6] *= 1.6  # Grocery
                cat_weights[1] *= 0.4  # Apparel down
            cat_weights /= cat_weights.sum()

            chosen_cat   = weighted_choice(list(CATEGORY_TAXONOMY.keys()), cat_weights.tolist())
            cat_products = df_product[df_product["Category"].str.lower() == chosen_cat.lower()]["ProductID"].tolist()
            if not cat_products:
                cat_products = product_ids
            prod_id = cat_products[rng.integers(0, len(cat_products))]

            cost  = prod_cost.get(prod_id, 500.0)
            lp    = prod_list.get(prod_id, cost * 1.3)
            cat   = prod_cat.get(prod_id, chosen_cat)

            # Festive multiplier affects quantity
            fest_mult, fest_name = get_festive_multiplier(d, cat)

            qty = int(rng.integers(1, 4))
            if fest_mult > 1.5:
                qty = int(rng.integers(1, 6))

            # Discount logic: increases over years (margin pressure story)
            base_disc_pct = {2020: 0.06, 2021: 0.08, 2022: 0.10, 2023: 0.13, 2024: 0.16}[year]
            disc_pct = float(rng.uniform(0, base_disc_pct * 2))
            if fest_name:
                disc_pct = min(disc_pct * 1.5, 0.60)

            unit_price    = round(lp * (1 - disc_pct), 2)
            disc_amount   = round((lp - unit_price) * qty, 2)
            gross_revenue = round(unit_price * qty, 2)
            shipping_cost = round(float(rng.uniform(0, 150)), 2) if rng.random() > 0.35 else 0.0
            cogs          = round(cost * qty, 2)
            net_profit    = round(gross_revenue - cogs - shipping_cost, 2)

            # Store: online channel grows over years
            online_prob = 0.30 + (year - 2020) * 0.06
            if covid:
                online_prob = max(online_prob, covid["online_shift"])
            if rng.random() < online_prob:
                online_stores = df_store[df_store["StoreType"] == "Online Only"]["StoreID"].tolist()
                store_id = online_stores[rng.integers(0, len(online_stores))] if online_stores else store_ids[rng.integers(0, len(store_ids))]
            else:
                store_id = store_ids[rng.integers(0, len(store_ids))]

            # SalesRep: null for online orders (~30%)
            rep_id = None
            if store_type.get(store_id) != "Online Only" and rng.random() > 0.031:
                rep_id = rep_ids[rng.integers(0, len(rep_ids))]

            # Promotion: 18% of orders have no promo
            promo_id = promo_ids[rng.integers(0, len(promo_ids))] if rng.random() > 0.18 else None

            # Customer
            cust_id = customer_ids[rng.integers(0, N_CUSTOMERS)]

            # Ship date (1-7 days after order)
            ship_lag = int(rng.integers(1, 8))
            ship_date = d + timedelta(days=ship_lag)

            order_id = f"ORD{order_counter:08d}"

            records.append({
                "OrderID":        order_id,
                "CustomerID":     cust_id,
                "ProductID":      prod_id,
                "StoreID":        store_id,
                "SalesRepID":     rep_id,
                "PromotionID":    promo_id,
                "OrderDate":      d.isoformat(),
                "ShipDate":       ship_date.isoformat(),
                "Year":           year,
                "Month":          month,
                "Category":       cat,
                "SubCategory":    df_product[df_product["ProductID"] == prod_id]["SubCategory"].values[0] if prod_id in df_product["ProductID"].values else "Unknown",
                "Quantity":       qty,
                "UnitPrice":      unit_price,
                "ListPrice":      lp,
                "DiscountPct":    round(disc_pct * 100, 2),
                "DiscountAmount": disc_amount,
                "GrossRevenue":   gross_revenue,
                "ShippingCost":   shipping_cost,
                "COGS":           cogs,
                "NetProfit":      net_profit,
                "PaymentMethod":  weighted_choice(PAYMENT_METHODS, PAYMENT_W),
                "Channel":        "Online" if store_type.get(store_id) == "Online Only" else "Offline",
                "FestivePeriod":  fest_name,
                "IsFestiveOrder": fest_name is not None,
                "IsCovidPeriod":  d in covid_lookup,
                "ReturnFlag":     False,  # populated later from returns
            })
            order_counter += 1

    df_month_check = pd.DataFrame(records)

    # ── INJECT DQ ISSUES ──

    # 1. Duplicate OrderIDs (~1.3%)
    n_dupes = int(len(df_month_check) * 0.013)
    dupe_idx = rng.choice(len(df_month_check), size=n_dupes, replace=False)
    dupes = df_month_check.iloc[dupe_idx].copy()
    dupes["ShipDate"] = dupes["OrderDate"]  # slightly different ship date
    df_month_check = pd.concat([df_month_check, dupes], ignore_index=True)

    # 2. UnitPrice = 0 (~0.4%) — comp/gift orders
    zero_idx = rng.choice(len(df_month_check), size=max(1, int(len(df_month_check)*0.004)), replace=False)
    df_month_check.iloc[zero_idx, df_month_check.columns.get_loc("UnitPrice")] = 0.0
    df_month_check.iloc[zero_idx, df_month_check.columns.get_loc("GrossRevenue")] = 0.0

    # 3. UnitPrice < 0 (~0.1%) — data entry errors
    neg_idx = rng.choice(len(df_month_check), size=max(1, int(len(df_month_check)*0.001)), replace=False)
    df_month_check.iloc[neg_idx, df_month_check.columns.get_loc("UnitPrice")] = round(float(rng.uniform(-500, -10)), 2)

    # 4. OrderDate > ShipDate (~0.3%) — system clock errors
    swap_idx = rng.choice(len(df_month_check), size=max(1, int(len(df_month_check)*0.003)), replace=False)
    for i in swap_idx:
        df_month_check.at[i, "ShipDate"] = (
            date.fromisoformat(df_month_check.at[i, "OrderDate"]) - timedelta(days=int(rng.integers(1, 4)))
        ).isoformat()

    # 5. Orphaned ProductIDs (~2%) — discontinued products
    orphan_idx = rng.choice(len(df_month_check), size=max(1, int(len(df_month_check)*0.02)), replace=False)
    for i in orphan_idx:
        df_month_check.at[i, "ProductID"] = f"PRD{rng.integers(90000, 99999):05d}"

    # 6. Orphaned StoreIDs (~1%)
    orphan_store_idx = rng.choice(len(df_month_check), size=max(1, int(len(df_month_check)*0.01)), replace=False)
    for i in orphan_store_idx:
        df_month_check.at[i, "StoreID"] = f"STR{rng.integers(9000, 9999):04d}"

    all_order_ids.extend(df_month_check["OrderID"].tolist())
    total_rows_generated += len(df_month_check)

    # ── WRITE partitioned Parquet ──
    for month in range(1, 13):
        df_m = df_month_check[df_month_check["Month"] == month].copy()
        if df_m.empty:
            continue
        out_dir = SOURCES / "parquet" / "sales" / f"year={year}" / f"month={month:02d}"
        out_dir.mkdir(parents=True, exist_ok=True)
        table = pa.Table.from_pandas(df_m, preserve_index=False)
        pq.write_table(table, out_dir / "sales.parquet", compression="snappy")

    print(f"         → {year}: {len(df_month_check):,} rows written (inc. DQ injections)")

print(f"\n    ✓ fact_sales — {total_rows_generated:,} total rows across 60 partition files")

# ─────────────────────────────────────────────
# 8. FACT_RETURNS → Partitioned Parquet
# ─────────────────────────────────────────────

print("\n[8/9] Building fact_returns (partitioned Parquet)...")

# Sample from all_order_ids for returns
n_returns = int(total_rows_generated * 0.05)
return_order_sample = rng.choice(all_order_ids, size=min(n_returns, len(all_order_ids)), replace=False)

return_records = []
for oid in return_order_sample:
    # Re-derive order date from OrderID counter (approximate)
    order_num = int(oid.replace("ORD",""))
    approx_year = 2020 + min(4, order_num // 270000)
    ret_date = date(approx_year,
                    int(rng.integers(1, 13)),
                    int(rng.integers(1, 28)))
    if ret_date > DATE_END:
        ret_date = DATE_END

    # DQ: 11% null ReturnReason (older records)
    reason = weighted_choice(RETURN_REASONS,
                             [0.18,0.12,0.20,0.15,0.10,0.08,0.07,0.05,0.05]) if rng.random() > 0.11 else None

    return_records.append({
        "ReturnID":       f"RET{len(return_records)+1:07d}",
        "OrderID":        oid,
        "ReturnDate":     ret_date.isoformat(),
        "ReturnReason":   reason,
        "RefundAmount":   round(float(rng.uniform(100, 45000)), 2),
        "RefundStatus":   weighted_choice(["Processed","Pending","Rejected"], [0.78,0.15,0.07]),
        "Year":           ret_date.year,
        "Month":          ret_date.month,
        "IsLateReturn":   rng.random() > 0.85,
        "ProcessingDays": int(rng.integers(1, 15)),
    })

df_returns = pd.DataFrame(return_records)

for year in range(2020, 2025):
    for month in range(1, 13):
        df_rm = df_returns[(df_returns["Year"] == year) & (df_returns["Month"] == month)]
        if df_rm.empty:
            continue
        out_dir = SOURCES / "parquet" / "returns" / f"year={year}" / f"month={month:02d}"
        out_dir.mkdir(parents=True, exist_ok=True)
        table = pa.Table.from_pandas(df_rm, preserve_index=False)
        pq.write_table(table, out_dir / "returns.parquet", compression="snappy")

print(f"    ✓ fact_returns — {len(df_returns):,} rows | null ReturnReason: {df_returns['ReturnReason'].isna().sum():,}")

# ─────────────────────────────────────────────
# 9. FINAL SUMMARY
# ─────────────────────────────────────────────

print("\n[9/9] Verifying outputs...")

checks = [
    (SOURCES / "json"    / "india_enrichment.json",      "JSON"),
    (SOURCES / "csv"     / "dim_product.csv",            "CSV"),
    (SOURCES / "csv"     / "dim_store.csv",              "CSV"),
    (SOURCES / "csv"     / "dim_promotion.csv",          "CSV"),
    (SOURCES / "postgres"/ "dim_customer.sql",           "SQL"),
    (SOURCES / "postgres"/ "dim_salesrep.sql",           "SQL"),
    (SOURCES / "excel"   / "finance_targets.xlsx",       "Excel"),
]

all_ok = True
for path, ftype in checks:
    exists = path.exists()
    size   = f"{path.stat().st_size / 1024:.1f} KB" if exists else "MISSING"
    status = "✓" if exists else "✗"
    if not exists:
        all_ok = False
    print(f"    {status} [{ftype}] {path.name} — {size}")

# Count parquet files
sales_parquets   = list((SOURCES / "parquet" / "sales").rglob("*.parquet"))
returns_parquets = list((SOURCES / "parquet" / "returns").rglob("*.parquet"))
print(f"    ✓ [Parquet] sales/   — {len(sales_parquets)} partition files")
print(f"    ✓ [Parquet] returns/ — {len(returns_parquets)} partition files")

print("\n" + "=" * 60)
print("  DATA GENERATION COMPLETE")
print("=" * 60)
print(f"""
  Sources generated:
    Parquet  → sources/parquet/  (sales + returns, partitioned)
    PostgreSQL → sources/postgres/ (customer + salesrep DDL+INSERTs)
    CSV      → sources/csv/      (product, store, promotion)
    JSON     → sources/json/     (India calendar enrichment)
    Excel    → sources/excel/    (finance targets)

  Data Quality Issues Injected (intentional):
    • Duplicate OrderIDs                   ~1.3% of sales
    • Null AgeGroup in customers            4.2%
    • Null Region in customers              2.8%
    • Null PromotionID in sales            18.0% (no-promo orders)
    • Null SalesRepID in sales              3.1% (online orders)
    • Null ReturnReason in returns         11.0%
    • UnitPrice = 0 (comp orders)           0.4%
    • UnitPrice < 0 (data entry errors)     0.1%
    • OrderDate > ShipDate (clock errors)   0.3%
    • Category casing variants in products  ~80 rows
    • Orphaned ProductIDs (discontinued)    2.0%
    • Orphaned StoreIDs (closed stores)     1.0%

  Next step:
    Run the PostgreSQL setup to load CRM tables.
    Then open Power BI Desktop and connect all 5 sources.
""")