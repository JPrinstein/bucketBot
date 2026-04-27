__all__ = ['split_end', 'season_reset']

from nextcord import Embed, Colour
from core.database import db
from core.utils import get_nick
import bot

PLACEMENT_POINTS = [25, 20, 18, 15, 14, 13, 12, 11, 10] + [8] * 5

db.ensure_table(dict(
    tname="qc_season_points",
    columns=[
        dict(cname="channel_id", ctype=db.types.int),
        dict(cname="user_id",    ctype=db.types.int),
        dict(cname="nick",       ctype=db.types.str),
        dict(cname="points",     ctype=db.types.int, notnull=True, default=0),
    ],
    primary_keys=["channel_id", "user_id"]
))

def _match_bonus(total_channel_matches: int, player_matches: int) -> int:
    if total_channel_matches >= 40:
        return player_matches // 10
    else:
        return player_matches // 5

async def _get_total_channel_matches(channel_id: int) -> int:
    rows = await db.select(['match_id'], 'qc_matches', where=dict(channel_id=channel_id))
    return len(rows)

async def _get_player_matches(channel_id: int) -> dict:
    rows = await db.select(['user_id'], 'qc_player_matches', where=dict(channel_id=channel_id))
    counts = {}
    for row in rows:
        counts[row['user_id']] = counts.get(row['user_id'], 0) + 1
    return counts

async def _post_season_leaderboard(ctx, channel, season_num: int = None):
    data = await db.select(
        ['user_id', 'nick', 'points'], 'qc_season_points',
        where=dict(channel_id=ctx.qc.id)
    )
    data.sort(key=lambda r: r['points'], reverse=True)
    if not data:
        return

    title = f"Season Leaderboard"
    if season_num:
        title += f" - Season {season_num}"

    lines = []
    for i, row in enumerate(data):
        pts = row['points']
        lines.append(f"**{i+1}.** {row['nick']} - **{pts} pts**")

    embed = Embed(title=title, description="\n".join(lines), colour=Colour(0xFFD700))
    await channel.send(embed=embed)


async def split_end(ctx, split_number: int = None):
    ctx.check_perms(ctx.Perms.ADMIN)

    guild = ctx.channel.guild

    split_channel_id  = getattr(ctx.qc.cfg, 'split_channel_id',  None)
    season_channel_id = getattr(ctx.qc.cfg, 'season_channel_id', None)

    if not split_channel_id:
        raise bot.Exc.SyntaxError(
            "Split results channel not configured. "
            "Use: `!set_channel_cfg {\"split_channel_id\": <channel_name>}`"
        )
    if not season_channel_id:
        raise bot.Exc.SyntaxError(
            "Season leaderboard channel not configured. "
            "Use: `!set_channel_cfg {\"season_channel_id\": <channel_name>}`"
        )

    split_channel  = split_channel_id if hasattr(split_channel_id, 'send') else guild.get_channel(int(split_channel_id))
    season_channel = season_channel_id if hasattr(season_channel_id, 'send') else guild.get_channel(int(season_channel_id))

    if not split_channel:
        raise bot.Exc.SyntaxError("Configured split channel not found.")
    if not season_channel:
        raise bot.Exc.SyntaxError("Configured season channel not found.")

    leaderboard    = await ctx.qc.get_lb()
    total_matches  = await _get_total_channel_matches(ctx.qc.id)
    player_matches = await _get_player_matches(ctx.qc.id)

    if not leaderboard:
        raise bot.Exc.NotFoundError("Leaderboard is empty - nothing to end.")

    split_results = []
    for i, row in enumerate(leaderboard):
        placement_pts = PLACEMENT_POINTS[i] if i < len(PLACEMENT_POINTS) else 0
        p_matches     = player_matches.get(row['user_id'], 0)
        bonus_pts     = _match_bonus(total_matches, p_matches) if p_matches > 0 else 0
        total_pts     = placement_pts + bonus_pts

        member = guild.get_member(row['user_id'])
        nick   = get_nick(member) if member else row['nick']

        split_results.append(dict(
            user_id       = row['user_id'],
            nick          = nick,
            placement_pts = placement_pts,
            bonus_pts     = bonus_pts,
            total_pts     = total_pts,
            rating        = row['rating'],
            position      = i + 1,
        ))

    title = "Split Results"
    if split_number:
        title += f" - Split {split_number}"

    lines = []
    for r in split_results:
        if r['bonus_pts']:
            pts_str = f"**{r['placement_pts']}** + **{r['bonus_pts']}** = **{r['total_pts']} pts**"
        else:
            pts_str = f"**{r['total_pts']} pts**"
        lines.append(f"**{r['position']}.** {r['nick']} - {r['rating']} rating - {pts_str}")

    bonus_rule = '≥40 matches: +1pt per 10 played' if total_matches >= 40 else '<40 matches: +1pt per 5 played'
    embed = Embed(
        title=title,
        description=f"Total matches this split: **{total_matches}** ({bonus_rule})\n\n" + "\n".join(lines),
        colour=Colour(0x50e3c2)
    )
    await split_channel.send(embed=embed)

    existing = await db.select(
        ['user_id', 'points'], 'qc_season_points',
        where=dict(channel_id=ctx.qc.id)
    )
    existing_map = {r['user_id']: r['points'] for r in existing}

    for r in split_results:
        if r['total_pts'] == 0:
            continue
        uid       = r['user_id']
        new_total = existing_map.get(uid, 0) + r['total_pts']
        if uid in existing_map:
            await db.update(
                'qc_season_points',
                dict(points=new_total, nick=r['nick']),
                keys=dict(channel_id=ctx.qc.id, user_id=uid)
            )
        else:
            await db.insert(
                'qc_season_points',
                dict(channel_id=ctx.qc.id, user_id=uid, nick=r['nick'], points=new_total)
            )

    await _post_season_leaderboard(ctx, season_channel, season_num=None)

    await ctx.qc.rating.reset()
    await bot.stats.reset_channel(ctx.qc.id)

    await ctx.success("Split ended. Results posted, season updated, ratings and stats reset.")


async def season_reset(ctx, season_number: int = None):
    ctx.check_perms(ctx.Perms.ADMIN)

    season_channel_id = getattr(ctx.qc.cfg, 'season_channel_id', None)
    if season_channel_id:
        ch = season_channel_id if hasattr(season_channel_id, 'send') else ctx.channel.guild.get_channel(getattr(season_channel_id, 'id', season_channel_id))
        if ch:
            # Post final leaderboard before reset
            

            # Post reset message
            title = "Season Reset"
            if season_number:
                title += f" - Season {season_number} Complete"
            embed = Embed(
                title=title,
                description="The season has been reset. All season points have been cleared.",
                colour=Colour(0xFF4444)
            )
            await ch.send(embed=embed)
            await _post_season_leaderboard(ctx, ch, season_num=season_number)
            

    await db.delete('qc_season_points', where=dict(channel_id=ctx.qc.id))
    await ctx.success("Season points have been reset.")