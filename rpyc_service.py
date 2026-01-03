import time
from socket import socket
from threading import Thread

import rpyc

from config import (
    XRAY_ASSETS_PATH,
    XRAY_EXECUTABLE_PATH,
    SINGBOX_ENABLED,
    SINGBOX_EXECUTABLE_PATH,
    SINGBOX_WORKING_DIR,
)
from logger import logger
from xray import XRayConfig, XRayCore

if SINGBOX_ENABLED:
    from singbox import SingBoxConfig, SingBoxCore


class CoreLogsHandler(object):
    """Generic logs handler for both Xray and Sing-box cores."""
    def __init__(self, core, callback: callable, interval: float = 0.6):
        self.core = core
        self.callback = callback
        self.interval = interval
        self.active = True
        self.thread = Thread(target=self.cast, daemon=True)
        self.thread.start()

    def stop(self):
        self.active = False
        self.thread.join(timeout=2)

    def cast(self):
        with self.core.get_logs() as logs:
            cache = ''
            last_sent_ts = 0
            while self.active:
                if time.time() - last_sent_ts >= self.interval and cache:
                    try:
                        self.callback(cache)
                    except Exception:
                        pass
                    cache = ''
                    last_sent_ts = time.time()

                if not logs:
                    time.sleep(0.2)
                    continue

                log = logs.popleft()
                cache += f'{log}\n'


# Alias for backward compatibility
XrayCoreLogsHandler = CoreLogsHandler


@rpyc.service
class XrayService(rpyc.Service):
    def __init__(self):
        self.core = None
        self.singbox_core = None
        self.connection = None

    def on_connect(self, conn):
        if self.connection:
            try:
                self.connection.ping()
                if self.connection.peer is not None:
                    logger.warning(
                        f'New connection rejected, already connected to {self.connection.peer}')
                return conn.close()
            except (EOFError, TimeoutError, AttributeError):
                if hasattr(self.connection, "peer"):
                    logger.warning(
                        f'Previous connection from {self.connection.peer} has lost')

        peer, _ = socket.getpeername(conn._channel.stream.sock)
        self.connection = conn
        self.connection.peer = peer
        logger.warning(f'Connected to {self.connection.peer}')

    def on_disconnect(self, conn):
        if conn is self.connection:
            logger.warning(f'Disconnected from {self.connection.peer}')

            if self.core is not None:
                self.core.stop()

            if self.singbox_core is not None:
                self.singbox_core.stop()

            self.core = None
            self.singbox_core = None
            self.connection = None

    @rpyc.exposed
    def start(self, config: str):
        if self.core is not None:
            self.stop()

        try:
            config = XRayConfig(config, self.connection.peer)
            self.core = XRayCore(executable_path=XRAY_EXECUTABLE_PATH,
                                 assets_path=XRAY_ASSETS_PATH)

            if self.connection and hasattr(self.connection.root, 'on_start'):
                @self.core.on_start
                def on_start():
                    try:
                        if self.connection:
                            self.connection.root.on_start()
                    except Exception as exc:
                        logger.debug('Peer on_start exception:', exc)
            else:
                logger.debug(
                    "Peer doesn't have on_start function on it's service, skipped")

            if self.connection and hasattr(self.connection.root, 'on_stop'):
                @self.core.on_stop
                def on_stop():
                    try:
                        if self.connection:
                            self.connection.root.on_stop()
                    except Exception as exc:
                        logger.debug('Peer on_stop exception:', exc)
            else:
                logger.debug(
                    "Peer doesn't have on_stop function on it's service, skipped")

            self.core.start(config)
        except Exception as exc:
            logger.error(exc)
            raise exc

    @rpyc.exposed
    def stop(self):
        if self.core:
            try:
                self.core.stop()
            except RuntimeError:
                pass
        self.core = None

    @rpyc.exposed
    def restart(self, config: str):
        config = XRayConfig(config, self.connection.peer)
        self.core.restart(config)

    @rpyc.exposed
    def fetch_xray_version(self):
        if self.core is None:
            raise ProcessLookupError("Xray has not been started")

        return self.core.version

    @rpyc.exposed
    def fetch_logs(self, callback: callable) -> XrayCoreLogsHandler:
        if self.core:
            logs = XrayCoreLogsHandler(self.core, callback)
            logs.exposed_stop = logs.stop
            logs.exposed_cast = logs.cast
            return logs

    # Sing-box methods
    @rpyc.exposed
    def singbox_start(self, config: str):
        if not SINGBOX_ENABLED:
            raise RuntimeError("Sing-box is not enabled on this node")

        if self.singbox_core is not None:
            self.singbox_stop()

        try:
            config = SingBoxConfig(config, self.connection.peer)
            self.singbox_core = SingBoxCore(
                executable_path=SINGBOX_EXECUTABLE_PATH,
                working_dir=SINGBOX_WORKING_DIR
            )

            if self.connection and hasattr(self.connection.root, 'on_singbox_start'):
                @self.singbox_core.on_start
                def on_start():
                    try:
                        if self.connection:
                            self.connection.root.on_singbox_start()
                    except Exception as exc:
                        logger.debug('Peer on_singbox_start exception:', exc)

            if self.connection and hasattr(self.connection.root, 'on_singbox_stop'):
                @self.singbox_core.on_stop
                def on_stop():
                    try:
                        if self.connection:
                            self.connection.root.on_singbox_stop()
                    except Exception as exc:
                        logger.debug('Peer on_singbox_stop exception:', exc)

            self.singbox_core.start(config)
        except Exception as exc:
            logger.error(exc)
            raise exc

    @rpyc.exposed
    def singbox_stop(self):
        if self.singbox_core:
            try:
                self.singbox_core.stop()
            except RuntimeError:
                pass
        self.singbox_core = None

    @rpyc.exposed
    def singbox_restart(self, config: str):
        if not SINGBOX_ENABLED:
            raise RuntimeError("Sing-box is not enabled on this node")

        config = SingBoxConfig(config, self.connection.peer)
        if self.singbox_core:
            self.singbox_core.restart(config)
        else:
            self.singbox_start(config)

    @rpyc.exposed
    def fetch_singbox_version(self):
        if not SINGBOX_ENABLED:
            return None

        if self.singbox_core is None:
            # Try to get version without starting
            temp_core = SingBoxCore(
                executable_path=SINGBOX_EXECUTABLE_PATH,
                working_dir=SINGBOX_WORKING_DIR
            )
            return temp_core.version

        return self.singbox_core.version

    @rpyc.exposed
    def is_singbox_enabled(self):
        return SINGBOX_ENABLED

    @rpyc.exposed
    def fetch_singbox_logs(self, callback: callable) -> CoreLogsHandler:
        if self.singbox_core:
            logs = CoreLogsHandler(self.singbox_core, callback)
            logs.exposed_stop = logs.stop
            logs.exposed_cast = logs.cast
            return logs
