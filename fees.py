# fees.py
"""
Backend helpers for the 'fees' collection.

- Provides aggregation utilities (totals_by_admin, admin_summary, grand_totals).
- Provides record_fee_from_deal(deal) to create a fee record exactly once when a deal is CREATED.
- Exposes small wrappers for editing/removing fees (used by owner-only commands).
- All outputs include a 'legacy' field with value 0 to remain compatible with older schemas.
"""

from typing import List, Dict, Any, Optional
from datetime import datetime
from bson import ObjectId

# Import DB helpers / collection. Adjust names if your db.py exports different symbols.
from db import COL_FEES, create_fee_record, list_fee_records, list_fees_by_admin, update_fee_record, delete_fee_record, db

# ---------- Aggregation / summary helpers ----------

async def totals_by_admin(limit: int = 0) -> List[Dict[str, Any]]:
    """
    Return totals grouped by admin_id from the 'fees' collection.
    Each item: { admin_id:int, admin_name:Optional[str], total:float, deals:int, legacy:0 }

    Uses a Mongo aggregation (scales to large datasets).
    If `limit` > 0, limits number of admins returned (after sorting by total desc).
    """
    pipeline = [
        {
            "$group": {
                "_id": {"admin_id": "$admin_id", "admin_name": {"$first": "$admin_name"}},
                "total": {"$sum": {"$ifNull": ["$fee", 0]}},
                "deals": {"$sum": 1},
            }
        },
        {"$sort": {"total": -1}},
    ]
    if limit and isinstance(limit, int) and limit > 0:
        pipeline.append({"$limit": int(limit)})

    out = []
    async for doc in COL_FEES.aggregate(pipeline):
        admin_id = doc["_id"].get("admin_id")
        admin_name = doc["_id"].get("admin_name")
        out.append({
            "admin_id": int(admin_id) if admin_id is not None else None,
            "admin_name": admin_name,
            "total": float(doc.get("total", 0.0)),
            "deals": int(doc.get("deals", 0)),
            "legacy": 0,
        })
    return out


async def admin_summary(admin_id: int) -> Dict[str, Any]:
    """
    Return summary for a single admin_id based on fee records.
    Shape: { admin_id, admin_name, total, deals, legacy }
    """
    # Use aggregation to compute quickly
    pipeline = [
        {"$match": {"admin_id": int(admin_id)}},
        {
            "$group": {
                "_id": {"admin_id": "$admin_id", "admin_name": {"$first": "$admin_name"}},
                "total": {"$sum": {"$ifNull": ["$fee", 0]}},
                "deals": {"$sum": 1},
            }
        },
    ]
    cursor = COL_FEES.aggregate(pipeline)
    doc = None
    async for d in cursor:
        doc = d
        break
    if not doc:
        return {"admin_id": int(admin_id), "admin_name": None, "total": 0.0, "deals": 0, "legacy": 0}
    return {
        "admin_id": int(admin_id),
        "admin_name": doc["_id"].get("admin_name"),
        "total": float(doc.get("total", 0.0)),
        "deals": int(doc.get("deals", 0)),
        "legacy": 0,
    }


async def grand_totals() -> Dict[str, Any]:
    """
    Return overall totals across the fees collection.
    Shape: { count: int, sum: float, legacy_sum: 0 }
    """
    pipeline = [
        {"$group": {"_id": None, "count": {"$sum": 1}, "sum": {"$sum": {"$ifNull": ["$fee", 0]}}}}
    ]
    cursor = COL_FEES.aggregate(pipeline)
    doc = None
    async for d in cursor:
        doc = d
        break
    if not doc:
        return {"count": 0, "sum": 0.0, "legacy_sum": 0}
    return {"count": int(doc.get("count", 0)), "sum": float(doc.get("sum", 0.0)), "legacy_sum": 0}


# ---------- Auto-record helper (call this when a deal is CREATED) ----------

async def record_fee_from_deal(deal: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Create a fee record derived from a deal object. This must be called exactly once when a deal is CREATED.
    Safety:
      - If the deal has a 'deal_id' and a fee record for that 'deal_id' already exists, this will NOT create another.
      - If there is no escrower_id, nothing is recorded.
    Expected deal fields (commonly available):
      - deal_id (unique identifier used to avoid duplicates)
      - escrower_id (int) -> will be recorded as admin_id
      - fee (numeric)
      - title / name (optional)
      - created_at (optional)
    Returns the created doc (with _id string) or None if skipped.
    """
    if not isinstance(deal, dict):
        return None

    escrower_id = deal.get("escrower_id")
    if escrower_id is None:
        return None

    deal_id = deal.get("deal_id")
    # Safety: don't duplicate by deal_id
    if deal_id is not None:
        existing = await COL_FEES.find_one({"deal_id": deal_id})
        if existing:
            return None

    # Normalize fee
    try:
        fee_val = float(deal.get("fee", 0.0))
    except Exception:
        fee_val = 0.0

    # admin_name: if you want to store a friendly name, allow callers to pass it via deal["escrower_name"]
    admin_name = deal.get("escrower_name") or deal.get("escrower_username") or None

    # record name/label for this fee record
    name = deal.get("title") or deal.get("name") or f"deal-{deal_id}" if deal_id is not None else (deal.get("title") or "deal")

    doc = {
        "admin_id": int(escrower_id),
        "admin_name": admin_name,
        "fee": fee_val,
        "name": str(name),
        "deal_id": deal_id,
        "created_at": deal.get("created_at") or datetime.utcnow(),
    }

    # Use db helper if available, otherwise insert directly
    try:
        created = await create_fee_record(int(escrower_id), fee_val, str(name))
        # create_fee_record may not set deal_id/admin_name; update those if needed
        if deal_id is not None:
            await COL_FEES.update_one({"_id": ObjectId(created["_id"])}, {"$set": {"deal_id": deal_id}})
        if admin_name:
            await COL_FEES.update_one({"_id": ObjectId(created["_id"])}, {"$set": {"admin_name": admin_name}})
        return created
    except Exception:
        # fallback: insert directly
        res = await COL_FEES.insert_one(doc)
        doc["_id"] = str(res.inserted_id)
        return doc


# ---------- Simple pass-throughs used by owner commands ----------
async def edit_fee(fee_id: str, updates: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Update a fee record. Returns updated doc or None if not found.
    Allowed updates: 'fee', 'name', 'admin_name' (and others handled by db.update wrapper).
    """
    return await update_fee_record(fee_id, updates)


async def remove_fee(fee_id: str) -> bool:
    """
    Delete a fee record by id. Returns True if removed.
    """
    return await delete_fee_record(fee_id)


# ---------- Compatibility helpers that call list_* helpers if they exist ----------
# These wrappers are small; in many places you might prefer direct db aggregation.
async def list_all_fees(limit: int = 100, skip: int = 0) -> List[Dict[str, Any]]:
    """
    Return raw fee documents (most recent first). Helpful for /listfees owner command.
    """
    return await list_fee_records(limit=limit, skip=skip)
