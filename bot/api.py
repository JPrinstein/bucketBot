# -*- coding: utf-8 -*-
"""Simple HTTP API for the website to query live bot state."""
import asyncio
import json
from aiohttp import web
import bot

async def handle_stats(request):
	in_queue = sum(len(q.queue) for q in bot.active_queues)
	in_match = sum(len(m.players) for m in bot.active_matches)
	data = {
		'in_queue': in_queue,
		'in_match': in_match,
		'active': in_queue + in_match,
	}
	return web.Response(
		text=json.dumps(data),
		content_type='application/json'
	)

async def start_api():
	app = web.Application()
	app.router.add_get('/api/live', handle_stats)
	runner = web.AppRunner(app)
	await runner.setup()
	site = web.TCPSite(runner, '0.0.0.0', 8765)
	await site.start()

def init():
	loop = asyncio.get_event_loop()
	loop.create_task(start_api())
