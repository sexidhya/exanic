# manage.py
from telethon import events, Button
from telethon.tl.functions.channels import GetParticipantRequest

# === CONFIGURATION ===
MAIN_GROUP_ID = -1002888180583  # main group
ESCROW_GROUP_IDS = {
    "-1002248727398": True,
    "-1002676048878": True,
    "-4885554031": True
}
FORCE_SUB_CHANNEL = "@exanic"  # channel for force-subscribe

def register(client):

    # ========== 1️⃣ AUTO DELETE EDITED MESSAGES ==========
    @client.on(events.MessageEdited(chats=[MAIN_GROUP_ID]))
    async def auto_delete_edits(event):
        """Deletes edited messages from non-admins in main group."""
        try:
            sender = await event.get_sender()
            if not sender:
                return

            # Check if sender is admin in that chat
            try:
                participant = await client(GetParticipantRequest(event.chat_id, sender.id))
                is_admin = getattr(participant.participant, "admin_rights", None)
            except Exception:
                # If we can't fetch participant info, be conservative and don't delete
                is_admin = True

            if not is_admin:
                await event.delete()
        except Exception as e:
            print(f"[manage.py][auto_delete_edits] Error: {e}")

    # ========== 2️⃣ FORCE SUBSCRIBE SYSTEM ==========
    @client.on(events.NewMessage(chats=[MAIN_GROUP_ID]))
    async def force_subscribe_handler(event):
        """Restrict new users until they join the specified channel."""
        try:
            user = await event.get_sender()
            if not user or user.bot:
                return

            # Skip admins (owner/devs)
            try:
                participant = await client(GetParticipantRequest(event.chat_id, user.id))
                is_admin = getattr(participant.participant, "admin_rights", None)
            except Exception:
                # If we cannot determine, assume not admin
                is_admin = None

            if is_admin:
                return

            # Check if user is already a member of the force-sub channel
            try:
                await client(GetParticipantRequest(FORCE_SUB_CHANNEL, user.id))
                return  # already in channel -> no restriction needed
            except Exception:
                # Not a member -> restrict (mute) them using edit_permissions
                # NOTE: use edit_permissions(chat, user, send_messages=False) to mute
                try:
                    await client.edit_permissions(event.chat_id, user.id, send_messages=False)
                except Exception as e:
                    print(f"[manage.py][force_subscribe] Failed to set permissions: {e}")

                # Reply with join + check buttons (appears under the user's message)
                buttons = [
                    [Button.url("🔗 Join Channel", f"https://t.me/{FORCE_SUB_CHANNEL.lstrip('@')}")],
                    [Button.inline("✅ Check", data=f"checksub:{user.id}")]
                ]

                await event.reply(
                    f"👋 {user.first_name}, please join our channel **{FORCE_SUB_CHANNEL}** to chat here.",
                    buttons=buttons,
                    parse_mode="markdown"
                )
        except Exception as e:
            print(f"[manage.py][force_subscribe] Error: {e}")

    import asyncio
    from telethon.tl.functions.channels import GetParticipantRequest

    @client.on(events.CallbackQuery(pattern=b"checksub:(\\d+)"))
    async def check_subscription(event):
        """Handles 'Check' button presses for Force Sub system and deletes the success message after delay."""
        try:
            user_id = int(event.pattern_match.group(1))
            chat_id = MAIN_GROUP_ID

            try:
                # Verify if user joined the channel
                await client(GetParticipantRequest(FORCE_SUB_CHANNEL, user_id))
                # Unmute the user
                await client.edit_permissions(chat_id, user_id)

                # Edit the inline message to show success
                msg = await event.edit("✅ You’ve joined the channel and are now unmuted. Welcome!")

                # Schedule deletion after 8 seconds (non-blocking)
                async def delete_later(message, delay=3):
                    await asyncio.sleep(delay)
                    try:
                        await message.delete()
                    except Exception:
                        pass

                asyncio.create_task(delete_later(msg, 3))

            except Exception:
                await event.answer("❌ You haven’t joined the required channel yet!", alert=True)

        except Exception as e:
            print(f"[manage.py][check_subscription] Error: {e}")
            try:
                await event.answer("⚠️ Error while checking subscription.", alert=True)
            except:
                pass
