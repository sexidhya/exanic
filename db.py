# db.py
from __future__ import annotations

from datetime import datetime, UTC
from typing import Iterable, List, Tuple

from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorCollection, AsyncIOMotorDatabase
from pymongo import ASCENDING, IndexModel
from pymongo.errors import OperationFailure

# -----------------------------------------------------------------------------
# Config
# -----------------------------------------------------------------------------
try:
    from config import MONGO_URI, DB_NAME
except Exception as e:
    raise RuntimeError("Missing config.MONGO_URI or config.DB_NAME") from e

# -----------------------------------------------------------------------------
# Client / DB / Collections
# -----------------------------------------------------------------------------
_client: AsyncIOMotorClient = AsyncIOMotorClient(MONGO_URI)
db: AsyncIOMotorDatabase = _client[DB_NAME]

COL_USERS: AsyncIOMotorCollection = db["users"]
COL_DEALS: AsyncIOMotorCollection = db["deals"]
COL_ESCROWERS: AsyncIOMotorCollection = db["escrowers"]
COL_COUNTS: AsyncIOMotorCollection = db["counts"]  # aggregated counters (global/daily/group_daily/escrower_daily)

# -----------------------------------------------------------------------------
# Helpers for safe index creation
# -----------------------------------------------------------------------------
def _key_tuple(spec: Iterable[Tuple[str, int]]) -> Tuple[Tuple[str, int], ...]:
    out: List[Tuple[str, int]] = []
    for kv in spec:
        if isinstance(kv, (list, tuple)) and len(kv) == 2:
            k, v = kv
            out.append((str(k), int(v)))
    return tuple(out)

def _has_equivalent_index(indexes_info: dict, *, key, unique=False, sparse=False) -> bool:
    """Return True if an index with the same key exists (we ignore name)."""
    target = _key_tuple(key)
    for _name, info in indexes_info.items():
        if _key_tuple(info.get("key", [])) == target:
            # option equality isn't strictly required to skip; treat as equivalent
            return True
    return False

async def _create_indexes_safely(col: AsyncIOMotorCollection, models: List[IndexModel]) -> None:
    if not models:
        return
    try:
        await col.create_indexes(models)
    except OperationFailure as e:
        # 85: IndexOptionsConflict, 86: IndexKeySpecsConflict → safe to ignore at startup
        if getattr(e, "code", None) not in (85, 86):
            raise

# -----------------------------------------------------------------------------
# Ensure Indexes
# -----------------------------------------------------------------------------
async def ensure_indexes() -> None:
    """
    Create all required indexes. Safe to call on every startup.
    """

    # ---------------------------
    # USERS
    # ---------------------------
    users_info = await COL_USERS.index_information()
    user_models: List[IndexModel] = []

    # Unique by user_id
    if not _has_equivalent_index(users_info, key=[("user_id", ASCENDING)], unique=True):
        user_models.append(IndexModel([("user_id", ASCENDING)], name="user_id_unique", unique=True))

    # Unique by username ONLY when present (avoid duplicate-key on null/missing)
    if "username_unique" not in users_info:
        user_models.append(
            IndexModel(
                [("username", ASCENDING)],
                name="username_unique",
                unique=True,
                partialFilterExpression={"username": {"$type": "string"}},
            )
        )

    # Useful lookup by username (non-unique). If unique exists, this isn't required,
    # but keeping it explicit can help the planner when unique is partial.
    if not _has_equivalent_index(users_info, key=[("username", ASCENDING)]):
        user_models.append(IndexModel([("username", ASCENDING)], name="username_lookup"))

    await _create_indexes_safely(COL_USERS, user_models)

    # ---------------------------
    # DEALS
    # ---------------------------
    deals_info = await COL_DEALS.index_information()
    deal_models: List[IndexModel] = []

    # Unique deal_id
    if not _has_equivalent_index(deals_info, key=[("deal_id", ASCENDING)], unique=True):
        deal_models.append(IndexModel([("deal_id", ASCENDING)], name="deal_id_unique", unique=True))

    # Status filters
    if not _has_equivalent_index(deals_info, key=[("status", ASCENDING)]):
        deal_models.append(IndexModel([("status", ASCENDING)], name="deal_status"))

    # For day-wise queries/backfills
    if not _has_equivalent_index(deals_info, key=[("status", ASCENDING), ("closed_at", ASCENDING)]):
        deal_models.append(IndexModel([("status", ASCENDING), ("closed_at", ASCENDING)], name="deal_status_closedat"))

    # Created_at (legacy/debug)
    if not _has_equivalent_index(deals_info, key=[("created_at", ASCENDING)]):
        deal_models.append(IndexModel([("created_at", ASCENDING)], name="deal_created_at"))

    # Per-escrower / per-group
    if not _has_equivalent_index(deals_info, key=[("escrower_id", ASCENDING)]):
        deal_models.append(IndexModel([("escrower_id", ASCENDING)], name="deal_escrower_id"))
    if not _has_equivalent_index(deals_info, key=[("form_chat_id", ASCENDING)]):
        deal_models.append(IndexModel([("form_chat_id", ASCENDING)], name="deal_form_chat_id"))

    # Usernames (normalized lower-case strings)
    if not _has_equivalent_index(deals_info, key=[("buyer_username", ASCENDING)]):
        deal_models.append(IndexModel([("buyer_username", ASCENDING)], name="deal_buyer_username"))
    if not _has_equivalent_index(deals_info, key=[("seller_username", ASCENDING)]):
        deal_models.append(IndexModel([("seller_username", ASCENDING)], name="deal_seller_username"))

    await _create_indexes_safely(COL_DEALS, deal_models)

    # ---------------------------
    # ESCROWERS
    # ---------------------------
    esc_info = await COL_ESCROWERS.index_information()
    esc_models: List[IndexModel] = []
    if not _has_equivalent_index(esc_info, key=[("user_id", ASCENDING)], unique=True):
        esc_models.append(IndexModel([("user_id", ASCENDING)], name="escrower_user_id_unique", unique=True))
    await _create_indexes_safely(COL_ESCROWERS, esc_models)

    # ---------------------------
    # COUNTS (scoped unique indexes)  ← FIXED
    # ---------------------------
    counts_info = await COL_COUNTS.index_information()
    counts_models: List[IndexModel] = []

    # Drop old conflicting global index if present (scope+date_utc for all scopes)
    # It causes duplicate key errors for escrower_daily/group_daily.
    if "counts_scope_date" in counts_info:
        try:
            await COL_COUNTS.drop_index("counts_scope_date")
        except OperationFailure:
            pass
        counts_info = await COL_COUNTS.index_information()

    # Generic scope index (non-unique)
    if not _has_equivalent_index(counts_info, key=[("scope", ASCENDING)]):
        counts_models.append(IndexModel([("scope", ASCENDING)], name="counts_scope"))

    # One doc for lifetime global (unique on scope='global')
    if "global_idx" not in counts_info:
        counts_models.append(
            IndexModel(
                [("scope", ASCENDING)],
                name="global_idx",
                unique=True,
                partialFilterExpression={"scope": "global"},
            )
        )

    # Daily totals (unique per day) — only for scope='daily'
    if "daily_idx" not in counts_info:
        counts_models.append(
            IndexModel(
                [("scope", ASCENDING), ("date_utc", ASCENDING)],
                name="daily_idx",
                unique=True,
                partialFilterExpression={"scope": "daily"},
            )
        )

    # Group daily (unique per group + day) — only for scope='group_daily'
    if "group_daily_idx" not in counts_info:
        counts_models.append(
            IndexModel(
                [("scope", ASCENDING), ("group_id", ASCENDING), ("date_utc", ASCENDING)],
                name="group_daily_idx",
                unique=True,
                partialFilterExpression={"scope": "group_daily"},
            )
        )

    # Escrower daily (unique per escrower + day) — only for scope='escrower_daily'
    if "escrower_daily_idx" not in counts_info:
        counts_models.append(
            IndexModel(
                [("scope", ASCENDING), ("escrower_id", ASCENDING), ("date_utc", ASCENDING)],
                name="escrower_daily_idx",
                unique=True,
                partialFilterExpression={"scope": "escrower_daily"},
            )
        )

    await _create_indexes_safely(COL_COUNTS, counts_models)

# -----------------------------------------------------------------------------
# Utilities
# -----------------------------------------------------------------------------
async def ping() -> dict:
    return await db.command("ping")

async def now_utc_iso() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")
