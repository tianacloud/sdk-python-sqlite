import asyncio
import importlib.util
import os
import sys
import unittest
from pathlib import Path

from .peer import ENDPOINT, FIXTURES, TOKEN, Peer

EXAMPLE = Path(__file__).resolve().parents[1] / "examples" / "sqlite.py"
SPEC = importlib.util.spec_from_file_location("sqlite_example", EXAMPLE)
DEMO = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(DEMO)


class ExampleTests(unittest.IsolatedAsyncioTestCase):
    async def run_example(self, *args, **settings):
        env = {k: v for k, v in os.environ.items() if not k.startswith("TIANA_")}
        env.update(settings)
        process = await asyncio.create_subprocess_exec(
            sys.executable,
            str(EXAMPLE),
            *args,
            env=env,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            stdout, stderr = await asyncio.wait_for(process.communicate(), 20)
        finally:
            if process.returncode is None:
                process.kill()
                await process.wait()
        return process.returncode, stdout.decode(), stderr.decode()

    def test_gateway_address(self):
        self.assertIsNone(DEMO.gateway_address(None))
        for value, expected in [
            ("localhost:443", ("localhost", 443)),
            ("127.0.0.1:8443", ("127.0.0.1", 8443)),
            ("[::1]:443", ("::1", 443)),
        ]:
            self.assertEqual(DEMO.gateway_address(value), expected)
        for invalid in [
            "",
            "host:0",
            "host:65536",
            "host:0443",
            "host:-1",
            "host:abc",
            "https://host:443",
            "user:secret@host:443",
            "host:443/path",
            "host:443?query",
            "::1:443",
            "[bad]:443",
            "host:443\n",
        ]:
            with self.subTest(invalid=invalid), self.assertRaises(ValueError) as caught:
                DEMO.gateway_address(invalid)
            self.assertNotIn("secret", str(caught.exception))

    async def test_help_and_configuration_fail_before_connect(self):
        code, out, err = await self.run_example("--help")
        self.assertEqual((code, err), (0, ""))
        for name in ["TIANA_ENDPOINT", "TIANA_TOKEN", "TIANA_CA_FILE", "TIANA_GATEWAY_ADDRESS"]:
            self.assertIn(name, out)
        code, out, err = await self.run_example()
        self.assertEqual((code, out), (1, ""))
        self.assertIn("TIANA_ENDPOINT must be set", err)
        async with Peer() as peer:
            settings = dict(
                TIANA_ENDPOINT=ENDPOINT,
                TIANA_TOKEN=TOKEN,
                TIANA_CA_FILE=str(FIXTURES / "gateway.pem"),
                TIANA_GATEWAY_ADDRESS=f"{peer.address[0]}:{peer.address[1]}",
            )
            for override, expected in [
                ({"TIANA_GATEWAY_ADDRESS": "user:secret@host:443"}, "TIANA_GATEWAY_ADDRESS"),
                ({"TIANA_CA_FILE": "missing-ca"}, "INVALID_TRUST_ROOT"),
                ({"TIANA_TOKEN": ""}, "TOKEN"),
            ]:
                code, out, err = await self.run_example(**(settings | override))
                self.assertEqual((code, out), (1, ""))
                self.assertIn(expected, err)
                self.assertNotIn("secret", err)
                self.assertNotIn(TOKEN, err)
            self.assertEqual(peer.connects, 0)

    @unittest.skipUnless(os.environ.get("TIANA_SQLITE_TEST_ADDRESS"), "requires disposable App")
    async def test_real_app_demo_repeated_runs(self):
        import ipaddress

        host, port = os.environ["TIANA_SQLITE_TEST_ADDRESS"].rsplit(":", 1)
        self.assertTrue(ipaddress.ip_address(host).is_loopback)
        async with Peer(backend=(host, int(port))) as peer:
            for _ in range(2):
                code, out, err = await self.run_example(
                    TIANA_ENDPOINT=ENDPOINT,
                    TIANA_TOKEN=TOKEN,
                    TIANA_CA_FILE=str(FIXTURES / "gateway.pem"),
                    TIANA_GATEWAY_ADDRESS=f"{peer.address[0]}:{peer.address[1]}",
                    TIANA_TOKEN_FILE="ignored-missing-file",
                    TIANA_DIAL_ADDRESS="ignored-invalid-address",
                )
                self.assertEqual((code, err), (0, ""))
                self.assertEqual(
                    out,
                    (
                        "committed row: id=9223372036854775807, label=hello Tiana, blob_bytes=2\n"
                        "rollback verified: rows=1\nsession closed\n"
                    ),
                )
            self.assertEqual(peer.connects, 2)
