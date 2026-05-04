__all__ = ['split_end', 'season_reset', 'season_archive_add', 'season_set_winner']

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

db.ensure_table(dict(
	tname="qc_season_archive",
	columns=[
		dict(cname="channel_id",    ctype=db.types.int),
		dict(cname="season_number", ctype=db.types.int),
		dict(cname="user_id",       ctype=db.types.int),
		dict(cname="nick",          ctype=db.types.str),
		dict(cname="points",        ctype=db.types.int, notnull=True, default=0),
	],
	primary_keys=["channel_id", "season_number", "user_id"]
))

db.ensure_table(dict(
	tname="qc_season_winners",
	columns=[
		dict(cname="channel_id",    ctype=db.types.int),
		dict(cname="season_number", ctype=db.types.int),
		dict(cname="position",      ctype=db.types.int),
		dict(cname="user_id",       ctype=db.types.int),
		dict(cname="nick",          ctype=db.types.str),
	],
	primary_keys=["channel_id", "season_number", "position"]
))


def _match_bonus(total_channel_matches: int, player_matches: int) -> int:
	"""
	< 40 total: +1 per 5 played starting at 10 (10=+1, 15=+2, 20=+3...)
	>= 40 total: +1 per 10 played starting at 10 (10=+1, 20=+2, 30=+3...)
	"""
	if player_matches < 10:
		return 0
	if total_channel_matches >= 40:
		return player_matches // 10
	else:
		return (player_matches - 5) // 5


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
	title = "Season Leaderboard"
	if season_num:
		title += f" - Season {season_num}"
	lines = []
	for i, row in enumerate(data):
		lines.append(f"**{i+1}.** {row['nick']} - **{row['points']} pts**")
	embed = Embed(title=title, description="\n".join(lines), colour=Colour(0xFFD700))
	await channel.send(embed=embed)


async def _archive_season(channel_id: int, season_number: int):
	data = await db.select(
		['user_id', 'nick', 'points'], 'qc_season_points',
		where=dict(channel_id=channel_id)
	)
	for row in data:
		existing = await db.select_one(
			('user_id',), 'qc_season_archive',
			where=dict(channel_id=channel_id, season_number=season_number, user_id=row['user_id'])
		)
		if existing:
			await db.update(
				'qc_season_archive',
				dict(points=row['points'], nick=row['nick']),
				keys=dict(channel_id=channel_id, season_number=season_number, user_id=row['user_id'])
			)
		else:
			await db.insert('qc_season_archive', dict(
				channel_id=channel_id,
				season_number=season_number,
				user_id=row['user_id'],
				nick=row['nick'],
				points=row['points'],
			))


async def split_end(ctx, split_number: int = None):
	ctx.check_perms(ctx.Perms.ADMIN)

	guild = ctx.channel.guild
	split_channel_id  = getattr(ctx.qc.cfg, 'split_channel_id',  None)
	season_channel_id = getattr(ctx.qc.cfg, 'season_channel_id', None)

	if not split_channel_id:
		raise bot.Exc.SyntaxError("Split results channel not configured.")
	if not season_channel_id:
		raise bot.Exc.SyntaxError("Season leaderboard channel not configured.")

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
		bonus_pts     = _match_bonus(total_matches, p_matches)
		total_pts     = placement_pts + bonus_pts
		member        = guild.get_member(row['user_id'])
		nick          = get_nick(member) if member else row['nick']
		split_results.append(dict(
			user_id=row['user_id'], nick=nick,
			placement_pts=placement_pts, bonus_pts=bonus_pts,
			total_pts=total_pts, rating=row['rating'], position=i+1,
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

	bonus_rule = '≥40 matches: +1pt per 10 played' if total_matches >= 40 else '<40 matches: +1pt per 5 played (min 10)'
	embed = Embed(
		title=title,
		description=f"Total matches this split: **{total_matches}** ({bonus_rule})\n\n" + "\n".join(lines),
		colour=Colour(0x50e3c2)
	)
	await split_channel.send(embed=embed)

	existing = await db.select(['user_id', 'points'], 'qc_season_points', where=dict(channel_id=ctx.qc.id))
	existing_map = {r['user_id']: r['points'] for r in existing}

	for r in split_results:
		if r['total_pts'] == 0:
			continue
		uid       = r['user_id']
		new_total = existing_map.get(uid, 0) + r['total_pts']
		if uid in existing_map:
			await db.update('qc_season_points', dict(points=new_total, nick=r['nick']),
				keys=dict(channel_id=ctx.qc.id, user_id=uid))
		else:
			await db.insert('qc_season_points',
				dict(channel_id=ctx.qc.id, user_id=uid, nick=r['nick'], points=new_total))

	await _post_season_leaderboard(ctx, season_channel, season_num=None)
	await ctx.qc.rating.reset()
	await bot.stats.reset_channel(ctx.qc.id)
	await ctx.success("Split ended. Results posted, season updated, ratings and stats reset.")


async def season_reset(ctx, season_number: int = None):
	ctx.check_perms(ctx.Perms.ADMIN)

	if not season_number:
		raise bot.Exc.SyntaxError("Season number is required.")

	await _archive_season(ctx.qc.id, season_number)

	season_channel_id = getattr(ctx.qc.cfg, 'season_channel_id', None)
	if season_channel_id:
		ch = season_channel_id if hasattr(season_channel_id, 'send') else ctx.channel.guild.get_channel(
			getattr(season_channel_id, 'id', season_channel_id)
		)
		if ch:
			embed = Embed(
				title=f"Season {season_number} Complete",
				description="The season has been reset. All season points have been cleared.",
				colour=Colour(0xFF4444)
			)
			await ch.send(embed=embed)
			await _post_season_leaderboard(ctx, ch, season_num=season_number)

	await db.delete('qc_season_points', where=dict(channel_id=ctx.qc.id))
	await ctx.success(f"Season {season_number} archived and points reset.")


async def season_archive_add(ctx, season_number: int, player: str, points: int):
	ctx.check_perms(ctx.Perms.ADMIN)
	guild = ctx.channel.guild
	member = None
	for m in guild.members:
		if str(m.id) in player or m.name.lower() == player.lower() or (m.nick and m.nick.lower() == player.lower()):
			member = m
			break
	if not member:
		raise bot.Exc.NotFoundError(f"Player '{player}' not found.")
	nick = get_nick(member)
	existing = await db.select_one(('user_id',), 'qc_season_archive',
		where=dict(channel_id=ctx.qc.id, season_number=season_number, user_id=member.id))
	if existing:
		await db.update('qc_season_archive', dict(points=points, nick=nick),
			keys=dict(channel_id=ctx.qc.id, season_number=season_number, user_id=member.id))
	else:
		await db.insert('qc_season_archive', dict(
			channel_id=ctx.qc.id, season_number=season_number,
			user_id=member.id, nick=nick, points=points))
	await ctx.success(f"Set {nick}'s Season {season_number} points to {points}.")


async def season_set_winner(ctx, season_number: int, player1: str, player2: str, player3: str):
	ctx.check_perms(ctx.Perms.ADMIN)
	guild = ctx.channel.guild

	async def resolve(name):
		for m in guild.members:
			if str(m.id) in name or m.name.lower() == name.lower() or (m.nick and m.nick.lower() == name.lower()):
				return m
		raise bot.Exc.NotFoundError(f"Player '{name}' not found.")

	members = [await resolve(p) for p in [player1, player2, player3]]
	for pos, member in enumerate(members, start=1):
		existing = await db.select_one(('position',), 'qc_season_winners',
			where=dict(channel_id=ctx.qc.id, season_number=season_number, position=pos))
		if existing:
			await db.update('qc_season_winners', dict(user_id=member.id, nick=get_nick(member)),
				keys=dict(channel_id=ctx.qc.id, season_number=season_number, position=pos))
		else:
			await db.insert('qc_season_winners', dict(
				channel_id=ctx.qc.id, season_number=season_number,
				position=pos, user_id=member.id, nick=get_nick(member)))

	nicks = [get_nick(m) for m in members]
	await ctx.success(f"Season {season_number} winners set: 🥇 {nicks[0]} 🥈 {nicks[1]} 🥉 {nicks[2]}")
