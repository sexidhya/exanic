# db.py
from __future__ import annotations
from datetime import datetime, timezone, date
UTC = timezone.utc
from typing import Iterable, List, Tuple, Any, Optional

from motor.motor_asyncio import (
    AsyncIOMotorClient,
    AsyncIOMotorCollection,
    AsyncIOMotorDatabase,
)
from pymongo import ASCENDING, IndexModel, ReturnDocument
from pymongo.errors import OperationFailure

# -----------------------------------------------------------------------------
# Config
# -----------------------------------------------------------------------------
try:
    from config import MONGO_URI, DB_NAME
except Exception as e:
    raise RuntimeError("Missing config.MONGO_URI or config.DB_NAME") from e

# -----------------------------------------------------------------------------
# Client / DB / Collections (public API unchanged + one new)
# -----------------------------------------------------------------------------
_client: AsyncIOMotorClient = AsyncIOMotorClient(MONGO_URI)
db: AsyncIOMotorDatabase = _client[DB_NAME]

COL_USERS: AsyncIOMotorCollection = db["users"]
COL_DEALS: AsyncIOMotorCollection = db["deals"]
COL_ESCROWERS: AsyncIOMotorCollection = db["escrowers"]
COL_COUNTS: AsyncIOMotorCollection = db["counts"]  # scoped counters (newer design)

# NEW: the old simple global counter (single doc)
# Shape: { _id: "1", amount: <float>, count: <int> }
COL_COUNT_SIMPLE: AsyncIOMotorCollection = db["count"]

# -----------------------------------------------------------------------------
# Index helpers
# -----------------------------------------------------------------------------
def _key_tuple(spec: Iterable[Tuple[str, int]]) -> Tuple[Tuple[str, int], ...]:
    out: List[Tuple[str, int]] = []
    for kv in spec:
        if isinstance(kv, (list, tuple)) and len(kv) == 2:
            k, v = kv
            out.append((str(k), int(v)))
    return tuple(out)

def _has_equivalent_index(indexes_info: dict, *, key, unique: bool = False) -> bool:
    target = _key_tuple(key)
    for _name, info in indexes_info.items():
        if _key_tuple(info.get("key", [])) == target:
            return True
    return False

async def _create_indexes_safely(col: AsyncIOMotorCollection, models: List[IndexModel]) -> None:
    if not models:
        return
    try:
        await col.create_indexes(models)
    except OperationFailure as e:
        if getattr(e, "code", None) not in (85, 86):  # existing/compatible
            raise

# -----------------------------------------------------------------------------
# Ensure Indexes (call at startup)
# -----------------------------------------------------------------------------
async def ensure_indexes() -> None:
    # USERS
    users_info = await COL_USERS.index_information()
    user_models: List[IndexModel] = []
    if not _has_equivalent_index(users_info, key=[("user_id", ASCENDING)], unique=True):
        user_models.append(IndexModel([("user_id", ASCENDING)], name="user_id_unique", unique=True))
    if "username_unique" not in users_info:
        user_models.append(IndexModel(
            [("username", ASCENDING)],
            name="username_unique",
            unique=True,
            partialFilterExpression={"username": {"$type": "string"}},
        ))
    if not _has_equivalent_index(users_info, key=[("username", ASCENDING)]):
        user_models.append(IndexModel([("username", ASCENDING)], name="username_lookup"))
    await _create_indexes_safely(COL_USERS, user_models)

    # DEALS
    deals_info = await COL_DEALS.index_information()
    deal_models: List[IndexModel] = []
    if not _has_equivalent_index(deals_info, key=[("deal_id", ASCENDING)], unique=True):
        deal_models.append(IndexModel([("deal_id", ASCENDING)], name="deal_id_unique", unique=True))
    for k, name in (
        ([("status", ASCENDING)], "deal_status"),
        ([("status", ASCENDING), ("closed_at", ASCENDING)], "deal_status_closedat"),
        ([("created_at", ASCENDING)], "deal_created_at"),
        ([("escrower_id", ASCENDING)], "deal_escrower_id"),
        ([("form_chat_id", ASCENDING)], "deal_form_chat_id"),
        ([("buyer_username", ASCENDING)], "deal_buyer_username"),
        ([("seller_username", ASCENDING)], "deal_seller_username"),
    ):
        if not _has_equivalent_index(deals_info, key=k):
            deal_models.append(IndexModel(k, name=name))
    await _create_indexes_safely(COL_DEALS, deal_models)

    # ESCROWERS
    esc_info = await COL_ESCROWERS.index_information()
    esc_models: List[IndexModel] = []
    if not _has_equivalent_index(esc_info, key=[("user_id", ASCENDING)], unique=True):
        esc_models.append(IndexModel([("user_id", ASCENDING)], name="escrower_user_id_unique", unique=True))
    await _create_indexes_safely(COL_ESCROWERS, esc_models)

    # COUNTS (scoped)
    counts_info = await COL_COUNTS.index_information()
    counts_models: List[IndexModel] = []
    if not _has_equivalent_index(counts_info, key=[("scope", ASCENDING)]):
        counts_models.append(IndexModel([("scope", ASCENDING)], name="counts_scope"))
    if "global_idx" not in counts_info:
        counts_models.append(IndexModel(
            [("scope", ASCENDING)],
            name="global_idx",
            unique=True,
            partialFilterExpression={"scope": "global"},
        ))
    if "daily_idx" not in counts_info:
        counts_models.append(IndexModel(
            [("scope", ASCENDING), ("date_utc", ASCENDING)],
            name="daily_idx",
            unique=True,
            partialFilterExpression={"scope": "daily"},
        ))
    if "group_daily_idx" not in counts_info:
        counts_models.append(IndexModel(
            [("scope", ASCENDING), ("group_id", ASCENDING), ("date_utc", ASCENDING)],
            name="group_daily_idx",
            unique=True,
            partialFilterExpression={"scope": "group_daily"},
        ))
    if "escrower_daily_idx" not in counts_info:
        counts_models.append(IndexModel(
            [("scope", ASCENDING), ("escrower_id", ASCENDING), ("date_utc", ASCENDING)],
            name="escrower_daily_idx",
            unique=True,
            partialFilterExpression={"scope": "escrower_daily"},
        ))
    await _create_indexes_safely(COL_COUNTS, counts_models)

    # COUNT (simple, one doc with _id="1")
    # _id is already unique; no extra index needed. Ensure doc exists:
    await COL_COUNT_SIMPLE.update_one(
        {"_id": "1"},
        {"$setOnInsert": {"amount": 0.0, "count": 0}},
        upsert=True,
    )

# -----------------------------------------------------------------------------
# Utilities
# -----------------------------------------------------------------------------
async def ping() -> dict:
    return await db.command("ping")

def _utc_day_str(dt: Optional[datetime] = None) -> str:
    if dt is None:
        return date.fromtimestamp(datetime.now(UTC).timestamp()).isoformat()
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.date().isoformat()

# -----------------------------------------------------------------------------
# Simple global counters (old design) — fast path for /gstats & logging
# -----------------------------------------------------------------------------
async def read_simple_global() -> tuple[float, int]:
    doc = await COL_COUNT_SIMPLE.find_one({"_id": "1"}) or {}
    return float(doc.get("amount", 0.0)), int(doc.get("count", 0))

async def inc_simple_global(amount_delta: float, count_delta: int = 1) -> None:
    await COL_COUNT_SIMPLE.update_one(
        {"_id": "1"},
        {"$inc": {"amount": float(amount_delta), "count": int(count_delta)}},
        upsert=True,
    )

async def set_simple_global(amount_total: float, count_total: int) -> dict:
    return await COL_COUNT_SIMPLE.find_one_and_update(
        {"_id": "1"},
        {"$set": {"amount": float(amount_total), "count": int(count_total)}},
        upsert=True,
        return_document=ReturnDocument.AFTER,
    )

# -----------------------------------------------------------------------------
# Scoped counters (new design) — global/daily/group_daily/escrower_daily
# -----------------------------------------------------------------------------
async def init_counts_documents() -> None:
    await COL_COUNTS.update_one(
        {"scope": "global"},
        {"$setOnInsert": {"deals": 0, "volume_main": 0.0, "updated_at": datetime.now(UTC)}},
        upsert=True,
    )

async def inc_counts_global(amount: float) -> None:
    await COL_COUNTS.update_one(
        {"scope": "global"},
        {"$inc": {"deals": 1, "volume_main": float(amount)},
         "$set": {"updated_at": datetime.now(UTC)}},
        upsert=True,
    )

async def inc_counts_daily(amount: float, when: Optional[datetime] = None) -> None:
    day = _utc_day_str(when)
    await COL_COUNTS.update_one(
        {"scope": "daily", "date_utc": day},
        {"$inc": {"deals": 1, "volume_main": float(amount)},
         "$set": {"updated_at": datetime.now(UTC)}},
        upsert=True,
    )

async def inc_counts_group_daily(amount: float, group_id: Optional[int], when: Optional[datetime] = None) -> None:
    if group_id is None:
        return
    day = _utc_day_str(when)
    await COL_COUNTS.update_one(
        {"scope": "group_daily", "group_id": int(group_id), "date_utc": day},
        {"$inc": {"deals": 1, "volume_main": float(amount)},
         "$set": {"updated_at": datetime.now(UTC)}},
        upsert=True,
    )

async def inc_counts_escrower_daily(amount: float, escrower_id: Optional[int], when: Optional[datetime] = None) -> None:
    if escrower_id is None:
        return
    day = _utc_day_str(when)
    await COL_COUNTS.update_one(
        {"scope": "escrower_daily", "escrower_id": int(escrower_id), "date_utc": day},
        {"$inc": {"deals": 1, "volume_main": float(amount)},
         "$set": {"updated_at": datetime.now(UTC)}},
        upsert=True,
    )

# -----------------------------------------------------------------------------
# ONE entry point to keep BOTH stores consistent (idempotent)
# -----------------------------------------------------------------------------
async def increment_counters_for_closed(deal: dict, *, amount_field: str = "main_amount") -> None:
    """
    Call this exactly once when a deal is marked closed.
    - Idempotent via 'counters_applied'=True gate on the deal document.
    - Updates BOTH the simple global doc and the scoped counters.
    - amount_field should match what you want to report everywhere ('main_amount' recommended).

    Expects deal with:
      _id, status='closed', closed_at (datetime, tz-aware preferred),
      form_chat_id (int), escrower_id (int), and amount field provided by 'amount_field'.
    """
    if not deal:
        return

    # Idempotency guard: only first caller performs increments
    res = await COL_DEALS.update_one(
        {"_id": deal["_id"], "counters_applied": {"$ne": True}},
        {"$set": {"counters_applied": True}},
    )
    if res.modified_count != 1:
        return  # already applied → do nothing

    when: datetime = deal.get("closed_at") or datetime.now(UTC)
    form_chat_id: Optional[int] = deal.get("form_chat_id")
    escrower_id: Optional[int] = deal.get("escrower_id")

    raw_amount: Any = deal.get(amount_field, 0.0)
    try:
        amount = float(raw_amount)
    except Exception:
        amount = 0.0

    # Update BOTH stores. Try a transaction; if unavailable, fall back to separate ops.
    session = await _client.start_session()
    try:
        async with session.start_transaction():
            # Simple global
            await COL_COUNT_SIMPLE.update_one(
                {"_id": "1"},
                {"$inc": {"amount": amount, "count": 1}},
                upsert=True,
                session=session,
            )
            # Scoped
            await COL_COUNTS.update_one(
                {"scope": "global"},
                {"$inc": {"deals": 1, "volume_main": amount},
                 "$set": {"updated_at": datetime.now(UTC)}},
                upsert=True,
                session=session,
            )
            day = _utc_day_str(when)
            await COL_COUNTS.update_one(
                {"scope": "daily", "date_utc": day},
                {"$inc": {"deals": 1, "volume_main": amount},
                 "$set": {"updated_at": datetime.now(UTC)}},
                upsert=True,
                session=session,
            )
            if form_chat_id is not None:
                await COL_COUNTS.update_one(
                    {"scope": "group_daily", "group_id": int(form_chat_id), "date_utc": day},
                    {"$inc": {"deals": 1, "volume_main": amount},
                     "$set": {"updated_at": datetime.now(UTC)}},
                    upsert=True,
                    session=session,
                )
            if escrower_id is not None:
                await COL_COUNTS.update_one(
                    {"scope": "escrower_daily", "escrower_id": int(escrower_id), "date_utc": day},
                    {"$inc": {"deals": 1, "volume_main": amount},
                     "$set": {"updated_at": datetime.now(UTC)}},
                    upsert=True,
                    session=session,
                )
    except Exception:
        # Fallback: best-effort separate ops (idempotency still protects from double counts)
        await inc_simple_global(amount, 1)
        await inc_counts_global(amount)
        await inc_counts_daily(amount, when)
        await inc_counts_group_daily(amount, form_chat_id, when)
        await inc_counts_escrower_daily(amount, escrower_id, when)
    finally:
        await session.end_session()

# db.py  (patched additions at bottom)

# ... keep your entire existing code above unchanged ...

# -----------------------------------------------------------------------------
# FEES COLLECTION — dedicated collection for manual/admin-managed fee records
# -----------------------------------------------------------------------------
COL_FEES: AsyncIOMotorCollection = db["fees"]

async def ensure_fees_indexes() -> None:
    """Ensure indexes for the fees collection."""
    info = await COL_FEES.index_information()
    models: List[IndexModel] = []
    if not _has_equivalent_index(info, key=[("admin_id", ASCENDING)]):
        models.append(IndexModel([("admin_id", ASCENDING)], name="fees_admin_id"))
    if not _has_equivalent_index(info, key=[("name", ASCENDING)]):
        models.append(IndexModel([("name", ASCENDING)], name="fees_name"))
    await _create_indexes_safely(COL_FEES, models)

# extend ensure_indexes() to call it
_old_ensure_indexes = ensure_indexes
async def ensure_indexes() -> None:
    await _old_ensure_indexes()
    await ensure_fees_indexes()

# -----------------------------------------------------------------------------
# CRUD HELPERS (async)
# -----------------------------------------------------------------------------
async def create_fee_record(admin_id: int, fee: float, name: str) -> dict:
    doc = {
        "admin_id": int(admin_id),
        "fee": float(fee),
        "name": str(name),
        "created_at": datetime.now(UTC),
    }
    res = await COL_FEES.insert_one(doc)
    doc["_id"] = str(res.inserted_id)
    return doc

async def get_fee_record(fee_id: str) -> Optional[dict]:
    from bson import ObjectId
    try:
        oid = ObjectId(fee_id)
    except Exception:
        return None
    doc = await COL_FEES.find_one({"_id": oid})
    if doc:
        doc["_id"] = str(doc["_id"])
    return doc

async def list_fee_records(limit: int = 50, skip: int = 0) -> List[dict]:
    cursor = COL_FEES.find().sort("created_at", -1).skip(skip).limit(limit)
    result = []
    async for d in cursor:
        d["_id"] = str(d["_id"])
        result.append(d)
    return result

async def list_fees_by_admin(admin_id: int) -> List[dict]:
    cursor = COL_FEES.find({"admin_id": int(admin_id)}).sort("created_at", -1)
    result = []
    async for d in cursor:
        d["_id"] = str(d["_id"])
        result.append(d)
    return result

async def update_fee_record(fee_id: str, updates: dict) -> Optional[dict]:
    from bson import ObjectId
    try:
        oid = ObjectId(fee_id)
    except Exception:
        return None
    allowed = {}
    if "fee" in updates:
        allowed["fee"] = float(updates["fee"])
    if "name" in updates:
        allowed["name"] = str(updates["name"])
    if not allowed:
        return await get_fee_record(fee_id)
    doc = await COL_FEES.find_one_and_update(
        {"_id": oid},
        {"$set": allowed},
        return_document=ReturnDocument.AFTER
    )
    if doc:
        doc["_id"] = str(doc["_id"])
    return doc

async def delete_fee_record(fee_id: str) -> bool:
    from bson import ObjectId
    try:
        oid = ObjectId(fee_id)
    except Exception:
        return False
    res = await COL_FEES.delete_one({"_id": oid})
    return res.deleted_count == 1
