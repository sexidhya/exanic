from motor.motor_asyncio import AsyncIOMotorClient
from config import MONGO_URI, DB_NAME


client = AsyncIOMotorClient(MONGO_URI)
db = client[DB_NAME]


COL_USERS = db["users"]
COL_DEALS = db["deals"]
COL_ESCROWERS = db["escrowers"]

from pymongo import IndexModel, ASCENDING
from pymongo.errors import OperationFailure

def _key_tuple(spec):
    """
    Normalize a MongoDB key spec (list of [("field", direction)]) into tuple form.
    Handles both list-of-tuples and list-of-lists.
    """
    out = []
    for kv in spec:
        if isinstance(kv, (list, tuple)) and len(kv) == 2:
            k, v = kv
            out.append((str(k), int(v)))
    return tuple(out)


def _has_equivalent_index(indexes_info, *, key, unique=False, sparse=False):
    """Return True if collection already has an index with same key+options,
    regardless of index name."""
    target_key = _key_tuple(key)
    for name, info in indexes_info.items():
        info_key = _key_tuple(info.get("key", []))
        if info_key == target_key:
            # If an index exists on same key, and uniqueness/sparse match (or are stricter), accept it
            if bool(info.get("unique", False)) == bool(unique) and bool(info.get("sparse", False)) == bool(sparse):
                return True
            # If options differ, we consider it conflicting and DO NOT recreate (to avoid error 85/86)
            return True
    return False

async def ensure_indexes():
    # Explicit models (names are nice-to-have but we won't rely on them to decide)
    user_idx_models = [
        IndexModel([("username", ASCENDING)], name="username_unique", unique=True, sparse=True),
        IndexModel([("user_id", ASCENDING)],   name="user_id_unique", unique=True, sparse=True),
    ]
    deal_idx_models = [
        IndexModel([("created_at", ASCENDING)],    name="deal_created_at"),
        IndexModel([("buyer_username", ASCENDING)],name="deal_buyer_username"),
        IndexModel([("seller_username", ASCENDING)],name="deal_seller_username"),
        IndexModel([("status", ASCENDING)],        name="deal_status"),
    ]
    escrower_idx_models = [
        IndexModel([("user_id", ASCENDING)], name="escrower_user_id_unique", unique=True),
    ]

    # USERS
    users_info = await COL_USERS.index_information()
    to_create = []
    for im in user_idx_models:
        doc = im.document
        key = doc["key"]
        unique = bool(doc.get("unique", False))
        sparse = bool(doc.get("sparse", False))
        if not _has_equivalent_index(users_info, key=key, unique=unique, sparse=sparse):
            to_create.append(im)
    if to_create:
        try:
            await COL_USERS.create_indexes(to_create)
        except OperationFailure as e:
            # 85 = IndexOptionsConflict, 86 = IndexKeySpecsConflict -> safe to ignore at startup
            if getattr(e, "code", None) not in (85, 86):
                raise

    # DEALS
    deals_info = await COL_DEALS.index_information()
    to_create = []
    for im in deal_idx_models:
        doc = im.document
        key = doc["key"]
        if not _has_equivalent_index(deals_info, key=key, unique=False, sparse=False):
            to_create.append(im)
    if to_create:
        try:
            await COL_DEALS.create_indexes(to_create)
        except OperationFailure as e:
            if getattr(e, "code", None) not in (85, 86):
                raise

    # ESCROWERS
    esc_info = await COL_ESCROWERS.index_information()
    to_create = []
    for im in escrower_idx_models:
        doc = im.document
        key = doc["key"]
        unique = bool(doc.get("unique", False))
        if not _has_equivalent_index(esc_info, key=key, unique=unique, sparse=False):
            to_create.append(im)
    if to_create:
        try:
            await COL_ESCROWERS.create_indexes(to_create)
        except OperationFailure as e:
            if getattr(e, "code", None) not in (85, 86):
                raise
