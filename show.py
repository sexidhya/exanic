from telethon import events, Button
from db import COL_DEALS

def _build_private_link(chat_id: int, msg_id: int) -> str:
    """Always build tg://resolve link for private groups."""
    return f"https://t.me/c/{chat_id}/{msg_id}"

def register(client):
    @client.on(events.NewMessage(pattern=r'^/s\s+(\S+)'))
    async def show_deal_form(event):
        deal_id = event.pattern_match.group(1).strip().upper()
        deal = await COL_DEALS.find_one({"deal_id": deal_id})

        if not deal:
            await event.reply("❌ Deal not found.")
            return

        chat_id = deal.get("form_chat_id")
        msg_id = deal.get("form_message_id")
        if not chat_id or not msg_id:
            await event.reply("⚠️ This deal has no form reference stored.")
            return

        link = _build_private_link(chat_id, msg_id)
        buttons = [[Button.url("🔗 View Original Form", link)]]

        txt = (
            f"📄 Deal `{deal_id}`\n"
            f"➥ Amount: {deal.get('amount', 0):.1f}$\n"
            f"➥ Escrower: {deal.get('escrower_name', 'N/A')}"
        )

        await event.reply(txt, buttons=buttons)
