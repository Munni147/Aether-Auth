"""
=============================================================================
Roblox Executor 向け 高速 HWID ホワイトリストシステム (FastAPI + Redis + discord.py)
=============================================================================
【機能概要】
1. FastAPI + Uvicorn:
   - Roblox ExecutorからのHWID認証リクエストを非同期で超高速処理 (O(1) Redisハッシュ照合)
   - 初回起動時の自動HWIDバインド機能
2. discord.py (UI / View):
   - /panel 管理者コマンドでサーバー上に永続ボタンパネルを設置
   - 🔄 [HWIDをリセット] (ephemeral=True): 自身のHWIDを即時消去
   - 🔑 [自分のキーを確認] (ephemeral=True): 割り当てられたキーとHWID状況を表示
   - ❓ [使い方ヘルプ] (ephemeral=True): Luaコード設定方法とトラブルシューティング
   - 管理者用 /genkey, /deletekey スラッシュコマンド
3. 永続View対応:
   - Bot再起動後もパネルボタンが動作するよう custom_id & timeout=None を設定
=============================================================================
"""

import asyncio
import os
import secrets
import string
import time
from datetime import datetime
from typing import Optional

import discord
from discord import app_commands
from discord.ext import commands
from fastapi import FastAPI, HTTPException, Query, status
from pydantic import BaseModel
import redis.asyncio as aioredis
import uvicorn

# =============================================================================
# ⚙️ 設定エリア (環境変数または直接書き換えてください)
# =============================================================================
DISCORD_BOT_TOKEN = os.getenv("DISCORD_BOT_TOKEN", "YOUR_DISCORD_BOT_TOKEN_HERE")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
API_HOST = os.getenv("API_HOST", "0.0.0.0")
API_PORT = int(os.getenv("PORT", os.getenv("API_PORT", 8000)))

# 認証成功時にクライアントに配信する実行用Luaスクリプト (保護対象スクリプト本体)
PROTECTED_SCRIPT_PAYLOAD = """
print("[SUCCESS] Whitelist Verified! Welcome to the premium script.")
game:GetService("StarterGui"):SetCore("SendNotification", {
    Title = "Whitelist System",
    Text = "認証に成功しました！スクリプトをロード中...",
    Duration = 5
})
-- ここに実際のゲーム機能コードを記述します
"""

# =============================================================================
# 🗄️ Redis & FastAPI 共通ステート
# =============================================================================
redis_client: Optional[aioredis.Redis] = None
app = FastAPI(
    title="Roblox Whitelist API",
    description="High-performance O(1) HWID Whitelist API backed by Redis",
    version="2.0.0"
)

class VerifyRequest(BaseModel):
    key: str
    hwid: str

class VerifyResponse(BaseModel):
    success: bool
    message: str
    script: Optional[str] = None

# =============================================================================
# 🌐 FastAPI エンドポイント (/verify)
# =============================================================================
@app.get("/health")
async def health_check():
    """死活監視用エンドポイント"""
    is_redis_ok = False
    if redis_client:
        try:
            await redis_client.ping()
            is_redis_ok = True
        except Exception:
            is_redis_ok = False
    return {"status": "ok", "redis": is_redis_ok, "time": time.time()}


@app.post("/verify", response_model=VerifyResponse)
@app.get("/verify", response_model=VerifyResponse)
async def verify_hwid(
    payload: Optional[VerifyRequest] = None,
    key: Optional[str] = Query(None),
    hwid: Optional[str] = Query(None)
):
    """
    Roblox Executor からのリクエストを受け取り、O(1) でHWIDを照合する
    - 初回実行時: 送信されたHWIDをキーに自動バインド
    - 2回目以降: 送信されたHWIDと登録済みHWIDが一致するか検証
    """
    if not redis_client:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="データベースが利用できません。"
        )

    req_key = (payload.key if payload and payload.key else key or "").strip()
    req_hwid = (payload.hwid if payload and payload.hwid else hwid or "").strip()

    if not req_key or not req_hwid:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="key と hwid の両方が必須です。"
        )

    # Redis構造: whitelist:{license_key} -> Hash {discord_id, hwid, created_at, status}
    redis_key = f"whitelist:{req_key}"
    data = await redis_client.hgetall(redis_key)

    # 1. キーが存在しない場合
    if not data:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="無効なライセンスキーです。Discordサーバーで取得してください。"
        )

    # ステータス確認
    is_active = data.get(b"status", b"active").decode("utf-8") == "active"
    if not is_active:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="このライセンスキーは現在停止中または無効化されています。"
        )

    stored_hwid = data.get(b"hwid", b"").decode("utf-8")

    # 2. HWID未登録の場合 (初回起動時 / リセット直後): 自動バインド
    if not stored_hwid:
        await redis_client.hset(redis_key, "hwid", req_hwid)
        await redis_client.hset(redis_key, "last_used", datetime.utcnow().isoformat())
        return VerifyResponse(
            success=True,
            message="新規端末(HWID)のバインドに成功しました！認証完了。",
            script=PROTECTED_SCRIPT_PAYLOAD
        )

    # 3. HWID照合
    if stored_hwid == req_hwid:
        await redis_client.hset(redis_key, "last_used", datetime.utcnow().isoformat())
        return VerifyResponse(
            success=True,
            message="HWID認証に成功しました。",
            script=PROTECTED_SCRIPT_PAYLOAD
        )
    else:
        # HWID不一致
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="HWIDが一致しません。PCを変更した場合はDiscordパネルから[HWIDをリセット]を行ってください。"
        )


# =============================================================================
# 🤖 Discord Bot UI / View 実装
# =============================================================================
class WhitelistPanelView(discord.ui.View):
    """
    チャンネル上に常駐するセルフサービス用パネルUI
    - timeout=None と custom_id を指定することでBot再起動後も永続的にリスニング可能
    """
    def __init__(self):
        super().__init__(timeout=None)

    # 🔄 [HWIDをリセット] ボタン
    @discord.ui.button(
        label="HWIDをリセット",
        style=discord.ButtonStyle.danger,
        emoji="🔄",
        custom_id="whitelist_view:reset_hwid"
    )
    async def reset_hwid_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer(ephemeral=True)

        if not redis_client:
            await interaction.followup.send("❌ データベースに接続できませんでした。", ephemeral=True)
            return

        user_id = str(interaction.user.id)
        license_key_bytes = await redis_client.get(f"user_keys:{user_id}")
        if not license_key_bytes:
            embed = discord.Embed(
                title="❌ キーが見つかりません",
                description="あなたのアカウントにはライセンスキーが発行されていません。\n管理者からキーの発行を受けてください。",
                color=discord.Color.red()
            )
            await interaction.followup.send(embed=embed, ephemeral=True)
            return

        license_key = license_key_bytes.decode("utf-8")
        redis_key = f"whitelist:{license_key}"

        # 現在のHWID情報を消去 (空文字に設定)
        await redis_client.hset(redis_key, "hwid", "")
        await redis_client.hset(redis_key, "reset_at", datetime.utcnow().isoformat())

        embed = discord.Embed(
            title="🔄 HWIDリセット完了",
            description=(
                "**登録されているHWIDを正常にクリアしました！**\n\n"
                "次回Executorでスクリプトを実行した際に、起動したPCのHWIDが**自動的に新しく登録**されます。\n"
                "キーを変更する必要はありません。"
            ),
            color=discord.Color.green(),
            timestamp=datetime.utcnow()
        )
        embed.add_field(name="対象キー", value=f"`||{license_key}||`", inline=False)
        embed.set_footer(text="Roblox Whitelist Self-Service Panel")

        await interaction.followup.send(embed=embed, ephemeral=True)

    # 🔑 [自分のキーを確認] ボタン
    @discord.ui.button(
        label="自分のキーを確認",
        style=discord.ButtonStyle.primary,
        emoji="🔑",
        custom_id="whitelist_view:check_key"
    )
    async def check_key_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer(ephemeral=True)

        if not redis_client:
            await interaction.followup.send("❌ データベースに接続できませんでした。", ephemeral=True)
            return

        user_id = str(interaction.user.id)
        license_key_bytes = await redis_client.get(f"user_keys:{user_id}")
        if not license_key_bytes:
            embed = discord.Embed(
                title="❌ キーが見つかりません",
                description="現在登録されているキーはありません。",
                color=discord.Color.orange()
            )
            await interaction.followup.send(embed=embed, ephemeral=True)
            return

        license_key = license_key_bytes.decode("utf-8")
        data = await redis_client.hgetall(f"whitelist:{license_key}")

        current_hwid = data.get(b"hwid", b"").decode("utf-8")
        status_str = data.get(b"status", b"active").decode("utf-8")
   　　　created_at = data.get(b"created_at", "不明".encode("utf-8")).decode("utf-8")

        embed = discord.Embed(
            title="🔑 ライセンスキー情報",
            description="あなたのホワイトリスト登録情報です (第三者にキーを共有しないでください)。",
            color=discord.Color.blue(),
            timestamp=datetime.utcnow()
        )
        embed.add_field(name="ライセンスキー", value=f"`||{license_key}||`", inline=False)
        embed.add_field(
            name="登録HWIDステータス",
            value=f"`{current_hwid[:12]}...` (登録済)" if current_hwid else "🟢 **未登録 (次回起動時に自動バインド)**",
            inline=True
        )
        embed.add_field(name="状態", value="✅ 有効" if status_str == "active" else "❌ 停止中", inline=True)
        embed.add_field(name="発行日時", value=created_at, inline=False)
        embed.set_footer(text="他人に見られないよう ephemeral(非公開) メッセージで送信されています")

        await interaction.followup.send(embed=embed, ephemeral=True)

    # ❓ [使い方ヘルプ] ボタン
    @discord.ui.button(
        label="使い方ヘルプ",
        style=discord.ButtonStyle.secondary,
        emoji="❓",
        custom_id="whitelist_view:help"
    )
    async def help_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        help_embed = discord.Embed(
            title="❓ スクリプト導入・利用ガイド",
            description="Roblox Executorでスクリプトを実行する手順です。",
            color=discord.Color.light_grey()
        )
        help_embed.add_field(
            name="1. キーを設定する",
            value=(
                "配布されたローダースクリプトの最上部にキーを記載します:\n"
                "```lua\n"
                "getgenv().Key = \"あなたのキー\"\n"
                "loadstring(game:HttpGet(\"https://your-api.com/loader.lua\"))()\n"
                "```"
            ),
            inline=False
        )
        help_embed.add_field(
            name="2. 実行と自動バインド",
            value="Executorで実行すると、現在のPCのハードウェア識別子(HWID)が自動検出され、初回のみキーにバインドされます。",
            inline=False
        )
        help_embed.add_field(
            name="3. PC変更時の対応",
            value="「HWID mismatch」と表示された場合は、このパネルの **[🔄 HWIDをリセット]** を押してから再度実行してください。",
            inline=False
        )
        await interaction.response.send_message(embed=help_embed, ephemeral=True)


# =============================================================================
# 🤖 Discord Client & スラッシュコマンド
# =============================================================================
intents = discord.Intents.default()
bot = commands.Bot(command_prefix="!", intents=intents)

@bot.event
async def on_ready():
    print(f"🤖 Discord Bot ログイン完了: {bot.user.name} ({bot.user.id})")
    # 永続Viewのリスナーを登録
    bot.add_view(WhitelistPanelView())
    try:
        synced = await bot.tree.sync()
        print(f"✅ スラッシュコマンド同期完了 ({len(synced)} コマンド)")
    except Exception as e:
        print(f"⚠️ コマンド同期失敗: {e}")

# 1. /panel コマンド (管理者専用)
@bot.tree.command(name="panel", description="【管理者専用】チャンネルにホワイトリスト管理パネルを設置します")
@app_commands.default_permissions(administrator=True)
async def panel_command(interaction: discord.Interaction):
    embed = discord.Embed(
        title="🛡️ ホワイトリスト管理パネル",
        description=(
            "スクリプト利用者向けのセルフサービスパネルです。\n"
            "下のボタンを押すことで、**キーの確認**や**HWIDのリセット**を行うことができます。\n\n"
            "※ボタンを押した際の返答はあなたにしか見えません (ephemeral=True)。"
        ),
        color=0x5865F2,
    )
    embed.add_field(
        name="🔄 HWIDをリセット",
        value="PCを買い替えた際や、HWIDエラーが出る場合に端末情報をクリアします。",
        inline=False
    )
    embed.add_field(
        name="🔑 自分のキーを確認",
        value="あなたに発行されたライセンスキーと登録状況を確認します。",
        inline=False
    )
    embed.add_field(
        name="❓ 使い方ヘルプ",
        value="Executorでのスクリプト実行方法・設定手順を表示します。",
        inline=False
    )
    embed.set_footer(text="Powered by FastAPI & Redis Whitelist System")

    # 永続Viewを持たせて送信
    await interaction.response.send_message(embed=embed, view=WhitelistPanelView())

# 2. /genkey コマンド (管理者専用)
@bot.tree.command(name="genkey", description="【管理者専用】指定ユーザーに新規ライセンスキーを発行します")
@app_commands.default_permissions(administrator=True)
@app_commands.describe(target_user="キーを割り当てるDiscordユーザー")
async def genkey_command(interaction: discord.Interaction, target_user: discord.User):
    await interaction.response.defer(ephemeral=True)

    if not redis_client:
        await interaction.followup.send("❌ Redisが利用できません。", ephemeral=True)
        return

    chars = string.ascii_uppercase + string.digits
    parts = ["".join(secrets.choice(chars) for _ in range(4)) for _ in range(3)]
    new_key = f"RBX-{parts[0]}-{parts[1]}-{parts[2]}"

    user_id = str(target_user.id)

    # 既存キーがあれば古いキーを削除
    old_key = await redis_client.get(f"user_keys:{user_id}")
    if old_key:
        await redis_client.delete(f"whitelist:{old_key.decode('utf-8')}")

    # Redisへ保存 (O(1))
    await redis_client.set(f"user_keys:{user_id}", new_key)
    await redis_client.hset(
        f"whitelist:{new_key}",
        mapping={
            "discord_id": user_id,
            "hwid": "",  # 未登録
            "status": "active",
            "created_at": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
        }
    )

    embed = discord.Embed(
        title="✅ 新規キー発行完了",
        color=discord.Color.green(),
        timestamp=datetime.utcnow()
    )
    embed.add_field(name="対象ユーザー", value=f"{target_user.mention} (`{target_user.id}`)", inline=False)
    embed.add_field(name="発行キー", value=f"`{new_key}`", inline=False)
    embed.add_field(name="初期HWID", value="未バインド (初回実行時に自動固定)", inline=False)

    await interaction.followup.send(embed=embed, ephemeral=True)

# 3. /deletekey コマンド (管理者専用)
@bot.tree.command(name="deletekey", description="【管理者専用】指定されたライセンスキーを削除・無効化します")
@app_commands.default_permissions(administrator=True)
@app_commands.describe(key="削除するライセンスキー")
async def deletekey_command(interaction: discord.Interaction, key: str):
    await interaction.response.defer(ephemeral=True)

    if not redis_client:
        await interaction.followup.send("❌ Redisが利用できません。", ephemeral=True)
        return

    key_clean = key.strip()
    data = await redis_client.hgetall(f"whitelist:{key_clean}")

    if not data:
        await interaction.followup.send(f"❌ キー `{key_clean}` は存在しません。", ephemeral=True)
        return

    discord_id = data.get(b"discord_id", b"").decode("utf-8")
    await redis_client.delete(f"whitelist:{key_clean}")
    if discord_id:
        await redis_client.delete(f"user_keys:{discord_id}")

    await interaction.followup.send(
        f"🗑️ キー `{key_clean}` (ユーザー: `<@{discord_id}>`) を正常に削除しました。",
        ephemeral=True
    )

# =============================================================================
# 🚀 並行動作メインエントリーポイント (FastAPI + Discord Bot)
# =============================================================================
async def main():
    global redis_client

    print("🚀 システム起動シーケンス開始...")
    
    # 1. Redis 非同期コネクションプールの初期化
    print(f"📦 Redis接続試行中 ({REDIS_URL})...")
    redis_client = aioredis.from_url(REDIS_URL, decode_responses=False)
    try:
        await redis_client.ping()
        print("✅ Redis接続成功: O(1) 高速キャッシュ準備完了")
    except Exception as e:
        print(f"⚠️ Redis接続警告: {e}")

    # 2. Uvicorn サーバー設定
    uvicorn_config = uvicorn.Config(
        app=app,
        host=API_HOST,
        port=API_PORT,
        log_level="info",
        access_log=False
    )
    server = uvicorn.Server(uvicorn_config)

    # 3. Discord Bot と Uvicorn サーバーを並行稼働 (asyncio.gather)
    print(f"🌐 FastAPI サーバー稼働準備: http://{API_HOST}:{API_PORT}")
    print("🤖 Discord Bot 起動中...")

    if DISCORD_BOT_TOKEN == "YOUR_DISCORD_BOT_TOKEN_HERE" or not DISCORD_BOT_TOKEN:
        print("⚠️ DISCORD_BOT_TOKEN を設定してください。FastAPIのみ単独待機します...")
        await server.serve()
    else:
        try:
            await asyncio.gather(
                server.serve(),
                bot.start(DISCORD_BOT_TOKEN)
            )
        except asyncio.CancelledError:
            print("🛑 シャットダウンシグナルを受信しました。")
        finally:
            if redis_client:
                await redis_client.close()
            print("👋 システムは正常に終了しました。")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n👋 ユーザーにより終了されました。")
