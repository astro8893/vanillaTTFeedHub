from aiohttp import web

from ..hub import Hub
from .auth import Authenticator, RateLimiter

HUB = web.AppKey("hub", Hub)
AUTH = web.AppKey("auth", Authenticator)
LIMITER = web.AppKey("limiter", RateLimiter)
