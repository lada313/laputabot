import asyncio
import ast
import json
import logging
import os
from pathlib import Path
import tempfile
import time
from types import SimpleNamespace as NS
import unittest
from unittest.mock import AsyncMock, patch

import security
import storage
import save_json

ROOT = Path(__file__).resolve().parents[1]


def handler_namespace():
    """Run the actual handler bodies with transport doubles, no broker SDK/network."""
    names = {'safe_answer', 'buy_from_button', 'sell_from_button', 'sell_price',
             'sell_price_type_handler', 'buy_price', 'price_type_handler', 'buy_amount',
             'commit_trade', 'load_portfolio', 'load_history', 'save_portfolio', 'save_history',
             'reset_portfolio', 'main'}
    tree = ast.parse((ROOT / 'main.py').read_text())
    selected = [node for node in tree.body if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name in names]
    ns = dict(vars(security), **{k: v for k, v in vars(storage).items() if not k.startswith('__')})
    ns.update(Update=object, ContextTypes=NS(DEFAULT_TYPE=object),
              ConversationHandler=NS(END=-1), BadRequest=type('BadRequest', (Exception,), {}),
              InlineKeyboardMarkup=lambda rows: rows, InlineKeyboardButton=lambda *a, **kw: kw,
              BUY_PRICE=0, BUY_PRICE_TYPE=1, BUY_AMOUNT=2,
              SELL_AMOUNT=3, SELL_PRICE=4, SELL_PRICE_TYPE=5,
              portfolio={}, history=[], TICKERS={}, price_history={}, last_signal={},
              get_lot_size=AsyncMock(return_value=1),
              datetime=__import__('datetime').datetime,
              logger=logging.getLogger('test'))
    exec(compile(ast.Module(body=selected, type_ignores=[]), str(ROOT / 'main.py'), 'exec'), ns)
    return ns


class InputsAndAccessTests(unittest.TestCase):
    def test_access_fails_closed(self):
        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaises(ValueError):
                security.allowed_user_ids()
        for raw in ('name', '-123', '0', '123,abc'):
            with patch.dict(os.environ, ALLOWED_USER_IDS=raw):
                with self.assertRaises(ValueError):
                    security.allowed_user_ids()
        with patch.dict(os.environ, ALLOWED_USER_IDS='123,456'):
            self.assertEqual(security.allowed_user_ids(), {123, 456})

    def test_private_owner_only(self):
        def update(uid=123, chat_id=123, chat_type='private', bot=False):
            return NS(effective_user=NS(id=uid, is_bot=bot), effective_chat=NS(id=chat_id, type=chat_type))
        self.assertTrue(security.authorized(update(), {123}))
        for item in (update(uid=456), update(chat_id=-123, chat_type='group'), update(bot=True), NS()):
            self.assertFalse(security.authorized(item, {123}))

    def test_money_rejects_adversarial_inputs(self):
        for value in ('-100', '+100', '100 rub', 'NaN', 'inf', '1e3', '0', '0.0000001', '9'*4000, '1000000001'):
            with self.subTest(value=value[:20]), self.assertRaises(ValueError):
                security.parse_money(value)
        self.assertEqual(security.parse_money('126,40'), 126.4)
        self.assertEqual(security.parse_money('0.000001'), 0.000001)

    def test_amounts_and_tickers(self):
        self.assertEqual(security.parse_amount('2л', 10), 20)
        for value in ('0', '-1', '1000001', '1.5', '9'*4000):
            with self.assertRaises(ValueError):
                security.parse_amount(value)
        for value in ('SBER/../../x', '[link](url)', 'A'*9, 'sber'):
            self.assertFalse(security.valid_ticker(value))

    def test_log_formatter_redacts_traceback_and_urls(self):
        secret = 'fake-token-for-test'
        record = logging.LogRecord('test', logging.ERROR, '', 1, 'request %s', ('https://api.telegram.org/bot'+secret+'/sendMessage',), None)
        with patch.dict(os.environ, TELEGRAM_TOKEN=secret):
            output = security.RedactingFormatter().format(record)
        self.assertNotIn(secret, output)
        self.assertNotIn('password', security.RedactingFormatter().format(logging.makeLogRecord({'msg':'https://user:password@github.com/repo'})))

    def test_every_update_passes_through_guard_first(self):
        tree = ast.parse((ROOT / 'main.py').read_text())
        main = next(n for n in tree.body if isinstance(n, ast.AsyncFunctionDef) and n.name == 'main')
        registrations = [n for n in ast.walk(main) if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute) and n.func.attr == 'add_handler']
        first = min(registrations, key=lambda n: n.lineno)
        self.assertIn('make_access_guard(allowed)', ast.unparse(first))
        self.assertIn('group=-100', ast.unparse(first))
        self.assertIn('TypeHandler(Update', ast.unparse(first))


class StorageTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)

    def test_corrupt_source_and_backup_survive(self):
        path = self.root / 'portfolio.json'
        storage.atomic_json(path, {'old': 1})
        storage.atomic_json(path, {'old': 2})
        self.assertEqual(json.loads(Path(str(path)+'.bak').read_text()), {'old': 1})
        path.write_text('{broken')
        with self.assertRaises(ValueError):
            storage.atomic_json(path, {'replacement': 3})
        self.assertEqual(path.read_text(), '{broken')
        self.assertEqual(json.loads(Path(str(path)+'.bak').read_text()), {'old': 1})

    def test_failed_replace_keeps_original(self):
        path = self.root / 'ledger.json'
        storage.atomic_json(path, {'value': 1})
        with patch('storage.os.replace', side_effect=OSError('disk unavailable')):
            with self.assertRaises(OSError):
                storage.atomic_json(path, {'value': 2})
        self.assertEqual(storage.read_json(path, {}), {'value': 1})
        self.assertEqual(list(self.root.glob('.ledger*')), [])

    def test_nonfinite_json_cannot_be_saved_or_loaded(self):
        path = self.root / 'ledger.json'
        with self.assertRaises(ValueError):
            storage.atomic_json(path, {'price': float('inf')})
        self.assertFalse(path.exists())
        path.write_text('{"price": NaN}')
        with self.assertRaises(ValueError):
            storage.read_json(path, {})

    def test_private_file_permissions(self):
        path = self.root / 'ledger.json'
        storage.atomic_json(path, {})
        storage.atomic_json(path, {'new': 1})
        self.assertEqual(path.stat().st_mode & 0o777, 0o600)
        self.assertEqual(Path(str(path)+'.bak').stat().st_mode & 0o777, 0o600)

    def test_ledger_failure_does_not_mutate_memory(self):
        ns = handler_namespace()
        ns['portfolio'].update(SBER={'price': 100, 'amount': 10})
        ns['atomic_json'] = unittest.mock.Mock(side_effect=OSError('disk full'))
        with self.assertRaises(OSError):
            ns['commit_trade']({}, {'action': 'sell'})
        self.assertEqual(ns['portfolio']['SBER']['amount'], 10)
        self.assertEqual(ns['history'], [])

    def test_ledger_persists_portfolio_and_history_together(self):
        ns = handler_namespace()
        ns['data_file'] = lambda name: str(self.root / name)
        ns['commit_trade']({'SBER': {'price': 100, 'amount': 10}}, {'action': 'buy'})
        saved = storage.read_json(self.root/'ledger.json', {})
        self.assertEqual(saved, {'portfolio': ns['portfolio'], 'history': ns['history']})
        ns['portfolio'].clear()
        ns['history'].clear()
        ns['load_portfolio']()
        ns['load_history']()
        self.assertEqual(ns['portfolio']['SBER']['amount'], 10)
        self.assertEqual(ns['history'], [{'action': 'buy'}])

    def test_corruption_does_not_reset_portfolio(self):
        ns = handler_namespace()
        path = self.root / 'ledger.json'
        path.write_text('{broken')
        ns['data_file'] = lambda name: str(self.root / name)
        ns['portfolio'].update(SBER={'price': 100, 'amount': 10})
        with self.assertRaises(ValueError):
            ns['load_portfolio']()
        self.assertEqual(ns['portfolio']['SBER']['amount'], 10)
        self.assertEqual(path.read_text(), '{broken')


class GitTests(unittest.IsolatedAsyncioTestCase):
    def test_destination_and_legacy_token_rejected(self):
        with patch.dict(os.environ, {}, clear=True):
            for url in ('https://evil.example/lada313/laputabot.git',
                        'https://user:password@github.com/lada313/laputabot.git',
                        'https://github.com/other/repo.git',
                        'https://github.com/lada313/laputabot.git?x=1'):
                with patch.dict(os.environ, GIT_REPO=url):
                    with self.assertRaises(ValueError):
                        save_json.safe_repo_url()
            with patch.dict(os.environ, GIT_REPO='https://github.com/lada313/laputabot.git'):
                self.assertEqual(save_json.safe_repo_url(), os.environ['GIT_REPO'])
                with patch.dict(os.environ, GIT_TOKEN='fake-secret'):
                    with self.assertRaises(ValueError):
                        save_json.safe_repo_url()

    async def test_no_private_file_is_staged(self):
        run = AsyncMock(return_value=0)
        with patch.dict(os.environ, {'GIT_SYNC':'1', 'GIT_REPO':'https://github.com/lada313/laputabot.git'}, clear=True), patch('save_json.run_git', run):
            for name in ('portfolio.json', 'history.json', 'open_trades.json', 'tickers.json', 'ledger.json', '*.json'):
                await save_json._do_push('Update '+name)
        run.assert_not_awaited()

    async def test_only_catalog_in_commit_and_no_secret_in_failure(self):
        run = AsyncMock(side_effect=[1,0,0,1])
        with patch.dict(os.environ, {'GIT_SYNC':'1', 'GIT_REPO':'https://github.com/lada313/laputabot.git'}, clear=True), patch('save_json.run_git', run):
            with self.assertRaisesRegex(RuntimeError, '^Git catalog push failed$'):
                await save_json._do_push('Update candidates.json')
        commit_args = run.await_args_list[2].args
        self.assertIn('--only', commit_args)
        self.assertEqual(commit_args[-2:], ('--', 'candidates.json'))

    async def test_git_timeout_kills_process(self):
        process = NS(wait=AsyncMock(side_effect=[asyncio.TimeoutError, 0]), kill=unittest.mock.Mock())
        with patch('save_json.asyncio.create_subprocess_exec', AsyncMock(return_value=process)):
            with self.assertRaises(asyncio.TimeoutError):
                await save_json.run_git('push', 'https://github.com/lada313/laputabot.git', 'HEAD:main')
        process.kill.assert_called_once()

    def test_disabled_by_default_and_queue_coalesces(self):
        with patch.dict(os.environ, {}, clear=True):
            self.assertFalse(save_json.git_enabled())
        queue = asyncio.Queue(maxsize=1)
        with patch.dict(os.environ, GIT_SYNC='1'), patch('save_json._GIT_QUEUE', queue):
            for _ in range(100):
                save_json.enqueue_git_push('Update candidates.json')
                save_json.enqueue_git_push('Update history.json')
        self.assertEqual(queue.qsize(), 1)


class HandlerTests(unittest.IsolatedAsyncioTestCase):
    def make_query(self, message_id=10, data='sell_price_type_single'):
        message = NS(chat=NS(id=123), message_id=message_id, reply_text=AsyncMock())
        return NS(data=data, message=message, answer=AsyncMock(), edit_message_text=AsyncMock())

    def context(self):
        return NS(user_data={'trade_kind':'sell', 'trade_started':time.monotonic(),
                            'trade_price_message':(123,10), 'sell_ticker':'SBER',
                            'sell_qty':10, 'sell_price_raw':120})

    async def test_old_button_cannot_sell(self):
        ns = handler_namespace()
        ns['portfolio']['SBER'] = {'price':100, 'amount':10}
        context = self.context()
        query = self.make_query(message_id=9)
        await ns['sell_price_type_handler'](NS(callback_query=query), context)
        self.assertEqual(ns['portfolio']['SBER']['amount'], 10)
        self.assertEqual(ns['history'], [])

    async def test_sale_rechecks_remaining_balance(self):
        ns = handler_namespace()
        ns['portfolio']['SBER'] = {'price':100, 'amount':5}
        context = self.context()
        await ns['sell_price_type_handler'](NS(callback_query=self.make_query()), context)
        self.assertEqual(ns['portfolio']['SBER']['amount'], 5)
        self.assertEqual(ns['history'], [])
        self.assertNotIn('sell_qty', context.user_data)

    async def test_sale_is_consumed_and_cannot_repeat(self):
        with tempfile.TemporaryDirectory() as directory:
            ns = handler_namespace()
            ns['data_file'] = lambda name: str(Path(directory) / name)
            ns['portfolio']['SBER'] = {'price':100, 'amount':20}
            context = self.context()
            update = NS(callback_query=self.make_query())
            await ns['sell_price_type_handler'](update, context)
            await ns['sell_price_type_handler'](update, context)
            self.assertEqual(ns['portfolio']['SBER']['amount'], 10)
            self.assertEqual(len(ns['history']), 1)
            self.assertEqual(storage.read_json(Path(directory)/'ledger.json', {})['portfolio']['SBER']['amount'], 10)

    async def test_expired_buy_cannot_commit(self):
        ns = handler_namespace()
        context = NS(user_data={'trade_kind':'buy', 'trade_started':time.monotonic()-601,
                                'buy_ticker':'SBER','buy_price_raw':100})
        update = NS(message=NS(text='10', reply_text=AsyncMock()))
        await ns['buy_amount'](update, context)
        self.assertEqual(ns['portfolio'], {})
        self.assertEqual(context.user_data, {})

    async def test_buy_persists_and_consumes_session(self):
        with tempfile.TemporaryDirectory() as directory:
            ns = handler_namespace()
            ns['data_file'] = lambda name: str(Path(directory) / name)
            context = NS(user_data={'trade_kind':'buy', 'trade_started':time.monotonic(),
                                    'buy_ticker':'SBER','buy_price_raw':100, 'price_is_total':False})
            update = NS(message=NS(text='10', reply_text=AsyncMock()))
            await ns['buy_amount'](update, context)
            await ns['buy_amount'](update, context)
            self.assertEqual(ns['portfolio']['SBER'], {'price':100, 'amount':10})
            self.assertEqual(len(ns['history']), 1)
            self.assertEqual(context.user_data, {})

    async def test_buy_rejects_invalid_quantity_without_mutation(self):
        ns = handler_namespace()
        context = NS(user_data={'trade_kind':'buy', 'trade_started':time.monotonic(),
                                'buy_ticker':'SBER','buy_price_raw':100})
        for text in ('-10', '1000001', '2л', 'NaN'):
            await ns['buy_amount'](NS(message=NS(text=text, reply_text=AsyncMock())), context)
        self.assertEqual(ns['portfolio'], {})
        self.assertEqual(ns['history'], [])

    async def test_heavy_action_single_flight_and_cooldown(self):
        started, release = asyncio.Event(), asyncio.Event()
        calls = []
        @security.limited_action(cooldown=30, timeout=5)
        async def work(update, context):
            calls.append(1)
            started.set()
            await release.wait()
        update = NS(message=NS(reply_text=AsyncMock()))
        task = asyncio.create_task(work(update, None))
        await started.wait()
        await work(update, None)
        release.set()
        await task
        await work(update, None)
        self.assertEqual(len(calls), 1)

    async def test_heavy_action_deadline(self):
        @security.limited_action(cooldown=0, timeout=0.001)
        async def work(update, context):
            await asyncio.Event().wait()
        update = NS(message=NS(reply_text=AsyncMock()))
        await work(update, None)
        update.message.reply_text.assert_awaited_once()


if __name__ == '__main__':
    unittest.main()
