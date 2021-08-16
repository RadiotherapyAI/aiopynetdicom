# Copyright (C) 2021 Radiotherapy AI Pty Ltd

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#     http://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import asyncio
import concurrent.futures
import logging
import signal
from typing import Optional

import pynetdicom.ae

from . import _ae, _consume

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s.%(msecs)d %(levelname)s: %(message)s",
    datefmt="%H:%M:%S",
)

# Loop handling code heavily inspired by Lynn Root's work at
# https://www.roguelynn.com/words/asyncio-we-did-it-wrong/


def start(port: int):
    with concurrent.futures.ThreadPoolExecutor() as executor:
        loop = asyncio.get_event_loop()
        loop.set_debug(True)

        ae = _ae.build_ae()

        handle_exception, shutdown = _create_exception_and_shutdown_handler(
            ae, executor
        )

        signals = (signal.SIGHUP, signal.SIGTERM, signal.SIGINT, signal.SIGQUIT)
        for s in signals:
            loop.add_signal_handler(
                s, lambda s=s: asyncio.create_task(shutdown(loop, sig=s))
            )
        loop.set_exception_handler(handle_exception)

        ae_queue: _ae.AeQueue = asyncio.Queue()
        loop.create_task(
            _ae.start_pynetdicom_server(
                ae=ae, port=port, ae_queue=ae_queue, executor=executor, loop=loop
            )
        )
        loop.create_task(_consume.consume(ae_queue))
        loop.run_forever()


def _create_exception_and_shutdown_handler(
    ae: pynetdicom.ae.ApplicationEntity,
    executor: concurrent.futures.ThreadPoolExecutor,
):
    def handle_exception(loop: asyncio.AbstractEventLoop, context):
        # context["message"] will always be there; but context["exception"] may not
        msg = context.get("exception", context["message"])
        # TODO: Make this an error log
        logging.error(f"Caught exception: {msg}")

        try:
            if "exception" in context:
                raise context["exception"]
        except Exception:  # pylint: disable = broad-except
            import sys

            from IPython.core.ultratb import ColorTB

            tb = "".join(ColorTB().structured_traceback(*sys.exc_info()))
            logging.error(tb)

        logging.info("Shutting down...")
        asyncio.create_task(shutdown(loop))

    async def shutdown(
        loop: asyncio.AbstractEventLoop,
        sig: Optional["signal.Signals"] = None,
    ):
        """Clean up tasks tied to the service's shutdown."""
        if sig:
            logging.info(f"Received exit signal {sig.name}...")

        logging.info("Shutting down pynetdicom server")
        ae.shutdown()

        tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]

        _ = [task.cancel() for task in tasks]
        logging.info(f"Cancelling {len(tasks)} outstanding tasks")
        await asyncio.gather(*tasks, return_exceptions=True)

        logging.info("Shutting down ThreadPoolExecutor")
        executor.shutdown(wait=False)

        threads = (
            executor._threads  # type: ignore  # pylint: disable = protected-access
        )
        logging.info(f"Releasing {len(threads)} threads from executor")
        for thread in threads:
            try:
                thread._tstate_lock.release()  # pylint: disable = protected-access
            except Exception:  # pylint: disable = broad-except
                pass

        logging.info("TODO: Flushing metrics")
        loop.stop()

    return handle_exception, shutdown
