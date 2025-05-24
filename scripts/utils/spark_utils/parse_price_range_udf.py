from pyspark.sql.functions import udf
from schemas.price_range_schema import price_range_schema
import re

def parse_price_range_str(price_str: str) -> dict:
    if not price_str:
        return {"price_min": None, "price_max": None}
    
    # Normalize input
    cleaned = price_str.replace("₫", "").replace(",", "").replace("–", "-").replace(".", "").strip()
    
    # Handle $$ and $$$
    if cleaned == "$$":
        return {"price_min": 100000.0, "price_max": 200000.0}
    if cleaned == "$$$":
        return {"price_min": 200000.0, "price_max": 500000.0}
    
    # Remove K and adjust multiplier
    multiplier = 1000.0 if "K" in price_str else 1.0
    cleaned = cleaned.replace("K", "").replace("+", "")
        
    range_match = re.match(r"^(\d+\.?\d*)\s*-\s*(\d+\.?\d*)$", cleaned)
    single_value_match = re.match(r"^(\d+\.?\d*)$", cleaned)
    
    if range_match:
        if (float(range_match.group(1)) == 0 and float(range_match.group(2)) == 0):
            return {"price_min": None, "price_max": None}

        return {
            "price_min": float(range_match.group(1)) * multiplier,
            "price_max": float(range_match.group(2)) * multiplier
        }

    elif single_value_match:
        value = float(single_value_match.group(1)) * multiplier
        return {
            "price_min": value,
            "price_max": str(value+500000) if "+" in price_str else value
        }
    
    return {"price_min": None, "price_max": None}

parse_price_range_udf = udf(parse_price_range_str, price_range_schema)