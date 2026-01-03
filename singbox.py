import atexit
import json
import re
import subprocess
import threading
from collections import deque
from contextlib import contextmanager

from config import (
    DEBUG,
    SSL_CERT_FILE,
    SSL_KEY_FILE,
    SINGBOX_INBOUNDS,
)
from logger import logger


class SingBoxConfig(dict):
    """
    Loads Sing-box config json.
    Handles Hysteria2, TUIC, and WireGuard protocols.
    """

    SUPPORTED_PROTOCOLS = {"hysteria2", "tuic", "wireguard"}

    def __init__(self, config: str, peer_ip: str):
        config = json.loads(config)

        self.ssl_cert = SSL_CERT_FILE
        self.ssl_key = SSL_KEY_FILE
        self.peer_ip = peer_ip

        super().__init__(config)
        self._apply_filters()

    def to_json(self, **json_kwargs):
        return json.dumps(self, **json_kwargs)

    def _apply_filters(self):
        """Filter inbounds based on SINGBOX_INBOUNDS configuration."""
        if not SINGBOX_INBOUNDS:
            return

        filtered_inbounds = []
        for inbound in self.get('inbounds', []):
            tag = inbound.get('tag')
            if tag in SINGBOX_INBOUNDS:
                filtered_inbounds.append(inbound)

        self['inbounds'] = filtered_inbounds


class SingBoxCore:
    def __init__(self,
                 executable_path: str = "/usr/local/bin/sing-box",
                 working_dir: str = "/var/lib/marzban-node"):
        self.executable_path = executable_path
        self.working_dir = working_dir

        self.version = self.get_version()
        self.process = None
        self.restarting = False

        self._logs_buffer = deque(maxlen=100)
        self._temp_log_buffers = {}
        self._on_start_funcs = []
        self._on_stop_funcs = []

        atexit.register(lambda: self.stop() if self.started else None)

    def get_version(self):
        try:
            cmd = [self.executable_path, "version"]
            output = subprocess.check_output(
                cmd, stderr=subprocess.STDOUT).decode('utf-8')
            m = re.search(r'version\s+(\d+\.\d+\.\d+)', output)
            if m:
                return m.group(1)
        except (subprocess.CalledProcessError, FileNotFoundError):
            return None

    def __capture_process_logs(self):
        def capture_and_debug_log():
            while self.process:
                output = self.process.stdout.readline()
                if output:
                    output = output.strip()
                    self._logs_buffer.append(output)
                    for buf in list(self._temp_log_buffers.values()):
                        buf.append(output)
                    logger.debug(output)

                elif not self.process or self.process.poll() is not None:
                    break

        def capture_only():
            while self.process:
                output = self.process.stdout.readline()
                if output:
                    output = output.strip()
                    self._logs_buffer.append(output)
                    for buf in list(self._temp_log_buffers.values()):
                        buf.append(output)

                elif not self.process or self.process.poll() is not None:
                    break

        if DEBUG:
            threading.Thread(target=capture_and_debug_log, daemon=True).start()
        else:
            threading.Thread(target=capture_only, daemon=True).start()

    @contextmanager
    def get_logs(self):
        buf = deque(self._logs_buffer, maxlen=100)
        buf_id = id(buf)
        try:
            self._temp_log_buffers[buf_id] = buf
            yield buf
        except (EOFError, TimeoutError):
            pass
        finally:
            if buf_id in self._temp_log_buffers:
                del self._temp_log_buffers[buf_id]

    @property
    def started(self):
        if not self.process:
            return False

        if self.process.poll() is None:
            return True

        return False

    def start(self, config: SingBoxConfig):
        if self.started is True:
            raise RuntimeError("Sing-box is started already")

        # Ensure log level is appropriate
        if config.get('log', {}).get('level') == 'silent':
            config['log']['level'] = 'warn'

        cmd = [
            self.executable_path,
            "run",
            '-c',
            'stdin'
        ]
        self.process = subprocess.Popen(
            cmd,
            cwd=self.working_dir,
            stdin=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            stdout=subprocess.PIPE,
            universal_newlines=True
        )
        self.process.stdin.write(config.to_json())
        self.process.stdin.flush()
        self.process.stdin.close()

        self.__capture_process_logs()

        # Execute on start functions
        for func in self._on_start_funcs:
            threading.Thread(target=func, daemon=True).start()

        logger.warning(f"Sing-box core {self.version} started")

    def stop(self):
        if not self.started:
            return

        self.process.terminate()
        try:
            self.process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            self.process.kill()
        self.process = None
        logger.warning("Sing-box core stopped")

        # Execute on stop functions
        for func in self._on_stop_funcs:
            threading.Thread(target=func, daemon=True).start()

    def restart(self, config: SingBoxConfig):
        if self.restarting is True:
            return

        self.restarting = True
        try:
            logger.warning("Restarting Sing-box core...")
            self.stop()
            self.start(config)
        finally:
            self.restarting = False

    def on_start(self, func: callable):
        self._on_start_funcs.append(func)
        return func

    def on_stop(self, func: callable):
        self._on_stop_funcs.append(func)
        return func
