# -*- coding: utf-8 -*-
import datetime
import pytz
from nextcord import Embed, Colour

from core.database import db
from core.client import dc
from core.console import log

ET = pytz.timezone('America/New_York')
DEFAULT_THRESHOLD = 5

db.ensure_table(dict(
	tname="qc_daily_match_counts",
	columns=[
		dict(cname="channel_id", ctype=db.types.int),
		dict(cname="user_id",    ctype=db.types.int),
		dict(cname="et_date",    ctype=db.types.str),
		dict(cname="count",      ctype=db.types.int, notnull=True, default=0),
	],
	primary_keys=["channel_id", "user_id", "et_date"]
))

db.ensure_table(dict(
	tname="qc_win_boosts",
	columns=[
		dict(cname="channel_id", ctype=db.types.int),
		dict(cname="user_id",    ctype=db.types.int),
		dict(cname="boost_date", ctype=db.types.str),
		dict(cname="active",     ctype=db.types.bool, notnull=True, default=0),
	],
	primary_keys=["channel_id", "user_id"]
))


def get_et_today() -> str:
	return datetime.datetime.now(ET).strftime("%Y-%m-%d")


def get_et_yesterday() -> str:
	return (datetime.datetime.now(ET) - datetime.timedelta(days=1)).strftime("%Y-%m-%d")


async def increment_match_count(channel_id: int, user_id: int):
	today = get_et_today()
	existing = await db.select_one(
		('count',), 'qc_daily_match_counts',
		where=dict(channel_id=channel_id, user_id=user_id, et_date=today)
	)
	if existing:
		await db.update(
			'qc_daily_match_counts',
			dict(count=existing['count'] + 1),
			keys=dict(channel_id=channel_id, user_id=user_id, et_date=today)
		)
	else:
		await db.insert('qc_daily_match_counts', dict(
			channel_id=channel_id, user_id=user_id, et_date=today, count=1
		))


async def player_has_boost(channel_id: int, user_id: int) -> bool:
	today = get_et_today()
	row = await db.select_one(
		('active', 'boost_date'), 'qc_win_boosts',
		where=dict(channel_id=channel_id, user_id=user_id)
	)
	return bool(row and row['active'] and row['boost_date'] == today)


async def _get_nick(channel_id: int, user_id: int) -> str:
	for guild in dc.guilds:
		member = guild.get_member(user_id)
		if member:
			return member.display_name
	row = await db.select_one(('nick',), 'qc_players', where=dict(channel_id=channel_id, user_id=user_id))
	return row['nick'] if row else str(user_id)


async def run_daily_reset(manual=False, use_today=False):
	import bot

	if use_today:
		yesterday = get_et_today()
		today = (datetime.datetime.now(ET) + datetime.timedelta(days=1)).strftime("%Y-%m-%d")
	else:
		today = get_et_today()
		yesterday = get_et_yesterday()

	for channel_id, qc in list(bot.queue_channels.items()):
		threshold = getattr(qc.cfg, 'boost_match_threshold', None) or DEFAULT_THRESHOLD

		yesterday_counts = await db.select(
			['user_id', 'count'], 'qc_daily_match_counts',
			where=dict(channel_id=channel_id, et_date=yesterday)
		)

		current_boosts = await db.select(
			['user_id', 'active', 'boost_date'], 'qc_win_boosts',
			where=dict(channel_id=channel_id)
		)
		boost_map = {r['user_id']: r for r in current_boosts}

		earned_boost = {r['user_id'] for r in yesterday_counts if r['count'] >= threshold}
		had_boost_yesterday = {
			uid for uid, r in boost_map.items()
			if r['active'] and r['boost_date'] == today
		}

		relevant_users = earned_boost | had_boost_yesterday

		# Always clear counts for the processed day
		await db.delete('qc_daily_match_counts', where=dict(channel_id=channel_id, et_date=yesterday))

		if not relevant_users:
			continue

		lines_nicknames = []
		lines_status = []
		for user_id in relevant_users:
			old_boost = user_id in had_boost_yesterday
			new_boost = user_id in earned_boost

			if user_id in boost_map:
				await db.update(
					'qc_win_boosts',
					dict(active=new_boost, boost_date=today),
					keys=dict(channel_id=channel_id, user_id=user_id)
				)
			else:
				await db.insert('qc_win_boosts', dict(
					channel_id=channel_id, user_id=user_id,
					boost_date=today, active=new_boost
				))

			nick = await _get_nick(channel_id, user_id)
			old_str = "2x" if old_boost else "1x"
			new_str = "2x" if new_boost else "1x"
			lines_nicknames.append(nick)
			lines_status.append(f"{old_str} → {new_str}")

		if not lines_nicknames:
			continue

		try:
			channel = dc.get_channel(channel_id)
			if channel:
				title = "New Day - Win Boost Update"
				if manual:
					title += " *(manual)*"
				embed = Embed(title=title, colour=Colour(0xF4A460))
				embed.add_field(name="Player", value="\n".join(lines_nicknames), inline=True)
				embed.add_field(name="Boost", value="\n".join(lines_status), inline=True)
				await channel.send(embed=embed)
		except Exception as e:
			log.error(f"daily_boost: failed to post to channel {channel_id}: {e}")


_last_et_date = None


@dc.event
async def on_think(frame_time):
	global _last_et_date
	today = get_et_today()
	if _last_et_date is None:
		_last_et_date = today
		return
	if today != _last_et_date:
		_last_et_date = today
		log.info("daily_boost: ET midnight crossed, running daily reset")
		try:
			await run_daily_reset()
		except Exception as e:
			log.error(f"daily_boost: error in daily reset: {e}")