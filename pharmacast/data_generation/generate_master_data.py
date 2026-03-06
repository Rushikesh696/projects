import pandas as pd
import numpy as np
import os
from datetime import datetime, timedelta
import random

random.seed(42)
np.random.seed(42)

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "../data/raw")
os.makedirs(OUTPUT_DIR, exist_ok=True)


# ─── Drug Master ────────────────────────────────────────────────────────────

DRUG_CATEGORIES = ["Antibiotic", "Painkiller", "Cardiac", "Diabetic", "Antiviral",
                   "Antifungal", "Neurological", "Gastrointestinal", "Respiratory", "Dermatology"]

DRUG_TYPES = ["Tablet", "Capsule", "Injection", "Syrup", "Cream", "Drops", "Inhaler"]

MANUFACTURERS = [
    "Sun Pharma", "Cipla", "Dr. Reddy's", "Lupin", "Aurobindo",
    "Zydus Cadila", "Torrent Pharma", "Alkem", "Abbott India", "Mankind Pharma"
]

DRUG_NAMES = [
    "Amoxicillin", "Azithromycin", "Ciprofloxacin", "Doxycycline", "Metronidazole",
    "Ibuprofen", "Paracetamol", "Diclofenac", "Naproxen", "Aspirin",
    "Atenolol", "Amlodipine", "Enalapril", "Metoprolol", "Losartan",
    "Metformin", "Glipizide", "Insulin", "Sitagliptin", "Empagliflozin",
    "Oseltamivir", "Acyclovir", "Remdesivir", "Ribavirin", "Favipiravir",
    "Fluconazole", "Itraconazole", "Clotrimazole", "Terbinafine", "Amphotericin",
    "Phenytoin", "Carbamazepine", "Levetiracetam", "Gabapentin", "Pregabalin",
    "Omeprazole", "Pantoprazole", "Ranitidine", "Ondansetron", "Domperidone",
    "Salbutamol", "Montelukast", "Theophylline", "Budesonide", "Tiotropium",
    "Hydrocortisone", "Betamethasone", "Clindamycin", "Mupirocin", "Tretinoin"
]


def generate_drug_master(n=200):
    drugs = []
    for i in range(1, n + 1):
        base_name = random.choice(DRUG_NAMES)
        drugs.append({
            "drug_id": f"D{i:04d}",
            "drug_name": f"{base_name}_{i}",
            "category": random.choice(DRUG_CATEGORIES),
            "drug_type": random.choice(DRUG_TYPES),
            "shelf_life_months": random.choice([12, 24, 36, 48, 60]),
            "cold_chain_required": random.choice([True, False]),
            "is_generic": random.choice([True, False]),
            "manufacturer": random.choice(MANUFACTURERS),
            "unit_price": round(random.uniform(5.0, 500.0), 2)
        })
    df = pd.DataFrame(drugs)
    df.to_csv(os.path.join(OUTPUT_DIR, "drug_master.csv"), index=False)
    print(f"Generated drug_master.csv with {len(df)} records.")
    return df


# ─── Region Master ──────────────────────────────────────────────────────────

REGIONS = [
    ("R001", "Mumbai", "Maharashtra", "West", "Tier 1"),
    ("R002", "Delhi", "Delhi", "North", "Tier 1"),
    ("R003", "Bangalore", "Karnataka", "South", "Tier 1"),
    ("R004", "Chennai", "Tamil Nadu", "South", "Tier 1"),
    ("R005", "Kolkata", "West Bengal", "East", "Tier 1"),
    ("R006", "Hyderabad", "Telangana", "South", "Tier 1"),
    ("R007", "Pune", "Maharashtra", "West", "Tier 2"),
    ("R008", "Ahmedabad", "Gujarat", "West", "Tier 2"),
    ("R009", "Jaipur", "Rajasthan", "North", "Tier 2"),
    ("R010", "Lucknow", "Uttar Pradesh", "North", "Tier 2"),
    ("R011", "Bhopal", "Madhya Pradesh", "Central", "Tier 2"),
    ("R012", "Patna", "Bihar", "East", "Tier 3"),
    ("R013", "Bhubaneswar", "Odisha", "East", "Tier 3"),
    ("R014", "Guwahati", "Assam", "East", "Tier 3"),
    ("R015", "Chandigarh", "Punjab", "North", "Tier 2"),
]


def generate_region_master():
    regions = []
    for r in REGIONS:
        regions.append({
            "region_id": r[0],
            "region_name": r[1],
            "state": r[2],
            "zone": r[3],
            "tier": r[4],
            "population_proxy": random.randint(500000, 20000000),
            "num_hospitals": random.randint(10, 200),
            "num_clinics": random.randint(50, 1000),
        })
    df = pd.DataFrame(regions)
    df.to_csv(os.path.join(OUTPUT_DIR, "region_master.csv"), index=False)
    print(f"Generated region_master.csv with {len(df)} records.")
    return df


# ─── Seasonal Events ────────────────────────────────────────────────────────

EVENTS = [
    ("Diwali", "festival", "High", ["North", "West", "Central"], ["Painkiller", "Gastrointestinal"]),
    ("Holi", "festival", "Medium", ["North", "Central"], ["Dermatology", "Respiratory"]),
    ("Durga Puja", "festival", "Medium", ["East"], ["Painkiller", "Gastrointestinal"]),
    ("Eid", "festival", "Medium", ["North", "West", "East"], ["Gastrointestinal"]),
    ("Christmas", "festival", "Low", ["South", "East"], ["Painkiller"]),
    ("Flu Season", "disease_season", "High", ["North", "East", "Central"], ["Antiviral", "Respiratory", "Antibiotic"]),
    ("Monsoon Season", "disease_season", "High", ["West", "South", "East"], ["Antibiotic", "Antifungal", "Gastrointestinal"]),
    ("Summer Heat", "disease_season", "Medium", ["North", "Central", "West"], ["Gastrointestinal", "Dermatology"]),
    ("Dengue Season", "disease_season", "High", ["South", "East", "West"], ["Antibiotic", "Painkiller"]),
    ("Republic Day", "holiday", "Low", ["North"], ["Painkiller"]),
    ("Independence Day", "holiday", "Low", ["North", "West"], ["Painkiller"]),
]

BASE_YEAR = 2022


def generate_seasonal_events():
    events = []
    event_id = 1
    for year in range(BASE_YEAR, BASE_YEAR + 3):
        for event in EVENTS:
            name, etype, impact, zones, categories = event
            start_date = datetime(year, random.randint(1, 11), random.randint(1, 28))
            end_date = start_date + timedelta(days=random.randint(3, 15))
            events.append({
                "event_id": f"E{event_id:04d}",
                "event_name": name,
                "event_type": etype,
                "demand_impact": impact,
                "affected_zones": "|".join(zones),
                "affected_drug_categories": "|".join(categories),
                "start_date": start_date.strftime("%Y-%m-%d"),
                "end_date": end_date.strftime("%Y-%m-%d"),
            })
            event_id += 1
    df = pd.DataFrame(events)
    df.to_csv(os.path.join(OUTPUT_DIR, "seasonal_events.csv"), index=False)
    print(f"Generated seasonal_events.csv with {len(df)} records.")
    return df


if __name__ == "__main__":
    generate_drug_master()
    generate_region_master()
    generate_seasonal_events()
    print("All master data generated successfully.")
