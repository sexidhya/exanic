# fee_cmd.py
"""
Handlers for fee-related commands (register-style).
- register(client) will attach all handlers.
- All commands are owner-only except /myfees which is for escrowers.
- /addfee exists as an OWNER-only manual backfill tool (per your request).
- Normal operation: fees are added automatically by calling fees.record_fee_from_deal(deal)
  from your deals creation flow (call once when deal is created).
"""

from telethon import events
from typing import Optional
import re

# DB/backend helpers
from db import create_fee_record, list_fee_records, list_fees_by_admin, update_fee_record, delete_fee_record
import fees as fees_backend  # backend module implemented above

# Permissions helpers (adjust import path if needed)
from permissions import is_owner, is_escrower  # replace 'permissions' with your module (utils/permissions)


def register(client):
    """Register fee-related commands on the given Telethon client."""

    # ----------------------------
    # /addfee <admin_id> <fee> <name>   (OWNER only) - manual backfill
    # ----------------------------
    @client.on(events.NewMessage(pattern=r"^/addfee\s+(\d+)\s+([\d.]+)\s+(.+)$"))
    async def addfee_cmd(event):
        if not await is_owner(event.sender_id):
            return await event.respond("❌ Owner-only command.")
        try:
            admin_id = int(event.pattern_match.group(1))
            fee = float(event.pattern_match.group(2))
            name = str(event.pattern_match.group(3)).strip()
        except Exception:
            return await event.respond("Usage: /addfee <admin_id> <fee> <name>")

        # create_fee_record should exist in db.py; it returns the created doc with _id string
        created = await create_fee_record(admin_id, fee, name)
        await event.respond(f"✅ Fee added (manual):\nID: `{created['_id']}`\nAdmin: `{admin_id}`\nFee: ${fee:.2f}\nName: `{name}`")


    # ----------------------------
    # /listfees   (OWNER only) -- raw listing
    # ----------------------------
    @client.on(events.NewMessage(pattern=r"^/listfees$"))
    async def listfees_cmd(event):
        if not await is_owner(event.sender_id):
            return await event.respond("❌ Owner-only command.")
        rows = await fees_backend.list_all_fees(limit=200)
        if not rows:
            return await event.respond("No fee records found.")
        lines = ["💰 Fee Records (most recent):"]
        for d in rows:
            fid = d.get("_id")
            # _id might be string if returned by db helper; otherwise convert
            lines.append(f"{fid} — {d.get('name','')} — ${float(d.get('fee',0)):.2f} — Admin {d.get('admin_id')}")
        await event.respond("\n".join(lines))


    # ----------------------------
    # /myfees   (Escrower can view their own fees)
    # ----------------------------
    @client.on(events.NewMessage(pattern=r"^/myfees$"))
    async def myfees_cmd(event):
        # Only escrowers can use this
        if not await is_escrower(event.sender_id):
            return await event.respond("❌ Only escrowers can use this command.")

        uid = int(event.sender_id)
        sender = await event.get_sender()
        sender_name = sender.first_name or sender.username or str(uid)

        # Fetch all fees for this escrower
        rows = await list_fees_by_admin(uid)
        if not rows:
            return await event.respond(f"💼 {sender_name}, you have no recorded fees yet.")

        total_fee = sum(float(d.get("fee", 0)) for d in rows)
        lines = [f"💼 **Fees for {sender_name}**\n **Total** → ${total_fee:.2f}"]

        # List all individual records
        for d in rows:
            deal_label = d.get("name", "Unnamed Deal")
            fee_value = float(d.get("fee", 0))
            lines.append(f" ")

        await event.respond("\n".join(lines))



    # ----------------------------
    # /editfee <fee_id> <new_fee> [new_name]   (OWNER only)
    # ----------------------------
    @client.on(events.NewMessage(pattern=r"^/editfee\s+([a-fA-F0-9]+)\s+([\d.]+)(?:\s+(.+))?$"))
    async def editfee_cmd(event):
        if not await is_owner(event.sender_id):
            return await event.respond("❌ Owner-only command.")
        fee_id = event.pattern_match.group(1)
        try:
            new_fee = float(event.pattern_match.group(2))
        except Exception:
            return await event.respond("Usage: /editfee <fee_id> <new_fee> [new_name]")
        new_name = event.pattern_match.group(3)
        updates = {"fee": new_fee}
        if new_name:
            updates["name"] = new_name.strip()
        updated = await fees_backend.edit_fee(fee_id, updates)
        if not updated:
            return await event.respond("❌ Fee not found or could not update.")
        return await event.respond(f"✅ Updated fee `{fee_id}` → ${updated['fee']:.2f} ({updated.get('name','')})")


    # ----------------------------
    # /delfee <fee_id>   (OWNER only)
    # ----------------------------
    @client.on(events.NewMessage(pattern=r"^/delfee\s+([a-fA-F0-9]+)$"))
    async def delfee_cmd(event):
        if not await is_owner(event.sender_id):
            return await event.respond("❌ Owner-only command.")
        fee_id = event.pattern_match.group(1)
        ok = await fees_backend.remove_fee(fee_id)
        if ok:
            return await event.respond(f"🗑️ Deleted fee `{fee_id}`.")
        return await event.respond("❌ Fee not found or could not delete.")


    # ----------------------------
    # /fees   (OWNER only) — grouped totals per admin
    # ----------------------------
    @client.on(events.NewMessage(pattern=r"^/fees$"))
    async def fees_cmd(event):
        if not await is_owner(event.sender_id):
            return await event.respond("❌ Owner-only command.")
        rows = await fees_backend.totals_by_admin()
        if not rows:
            return await event.respond("No fees recorded yet.")
        lines = ["**Fees Earned (All-Time):**\n"]
        for r in rows:
            name = r.get("admin_name") or str(r.get("admin_id"))
            uid = r.get("admin_id")
            total = float(r.get("total", 0.0))
            deals = int(r.get("deals", 0)) 
            lines.append(f"{name} → ${total:.2f}")
        await event.respond("\n".join(lines))


    # ----------------------------
    # /feestats  (OWNER only) — grand totals
    # ----------------------------
    @client.on(events.NewMessage(pattern=r"^/feestats$"))
    async def feestats_cmd(event):
        if not await is_owner(event.sender_id):
            return await event.respond("❌ Owner-only command.")
        gt = await fees_backend.grand_totals()
        await event.respond(f"📊 {gt['count']} fee records • Sum: ${gt['sum']:.2f}")
