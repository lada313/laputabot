"""Optional real PTB pipeline and Flask checks. No external requests are made."""
import json
import unittest
from unittest.mock import AsyncMock

try:
    from telegram import Update
    from telegram.ext import ApplicationBuilder, CommandHandler, CallbackQueryHandler, MessageHandler, TypeHandler, filters
    from telegram.request import BaseRequest
    from keep_alive import app
except ImportError:
    BaseRequest = object
    HAS_DEPS = False
else:
    HAS_DEPS = True

from security import make_access_guard


class LocalRequest(BaseRequest):
    @property
    def read_timeout(self):
        return 5

    async def initialize(self):
        pass

    async def shutdown(self):
        pass

    async def do_request(self, url, method, request_data=None, **kwargs):
        if url.endswith('/getMe'):
            result = {'id':999,'is_bot':True,'first_name':'Test','username':'test_bot'}
        elif url.endswith('/answerCallbackQuery'):
            result = True
        else:
            raise AssertionError('Unexpected network request')
        return 200, json.dumps({'ok':True, 'result':result}).encode()


@unittest.skipUnless(HAS_DEPS, 'Optional Telegram/Flask dependencies not installed')
class IntegrationTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.application = (ApplicationBuilder().token('999:local-test-token')
                            .request(LocalRequest()).get_updates_request(LocalRequest()).build())
        self.application.add_handler(TypeHandler(Update, make_access_guard({123})), group=-100)
        self.command = AsyncMock()
        self.callback = AsyncMock()
        self.message = AsyncMock()
        self.application.add_handler(CommandHandler('start', self.command))
        self.application.add_handler(CallbackQueryHandler(self.callback))
        self.application.add_handler(MessageHandler(filters.TEXT, self.message))
        await self.application.initialize()
        self.addAsyncCleanup(self.application.shutdown)

    def update(self, uid, callback=False, command=True, chat_type='private'):
        user = {'id':uid,'is_bot':False,'first_name':'Test'}
        message = {'message_id':1, 'date':1, 'chat':{'id':uid,'type':chat_type},
                   'from':user,'text':'/start' if command else '100'}
        if command:
            message['entities'] = [{'type':'bot_command','offset':0,'length':6}]
        if callback:
            body = {'update_id':1,'callback_query':{'id':'query','from':user,'chat_instance':'instance',
                                                  'message':message,'data':'remove_SBER'}}
        else:
            body = {'update_id':1,'message':message}
        return Update.de_json(body, self.application.bot)

    async def test_outsider_cannot_reach_any_handler(self):
        for update in [self.update(456), self.update(456, callback=True), self.update(456, command=False)]:
            await self.application.process_update(update)
        self.command.assert_not_awaited()
        self.callback.assert_not_awaited()
        self.message.assert_not_awaited()

    async def test_owner_command_passes_guard(self):
        await self.application.process_update(self.update(123))
        self.command.assert_awaited_once()

    async def test_owner_group_is_rejected(self):
        await self.application.process_update(self.update(123, chat_type='group'))
        self.command.assert_not_awaited()

    async def test_owner_callback_passes_guard(self):
        await self.application.process_update(self.update(123, callback=True))
        self.callback.assert_awaited_once()

    async def test_health_routes(self):
        with app.test_client() as client:
            self.assertEqual(client.get('/health').status_code, 200)
            self.assertEqual(client.get('/').data, b'OK')
