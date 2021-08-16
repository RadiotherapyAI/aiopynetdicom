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
from typing import Union

import pydicom
import pynetdicom
import pynetdicom.ae
import pynetdicom.association
import pynetdicom.pdu_primitives

EventTuple = Union[pynetdicom.evt.NotificationEvent, pynetdicom.evt.InterventionEvent]
Primitive = Union[
    pynetdicom.pdu_primitives.A_ABORT,
    pynetdicom.pdu_primitives.A_ASSOCIATE,
    pynetdicom.pdu_primitives.A_P_ABORT,
    pynetdicom.pdu_primitives.A_RELEASE,
    pynetdicom.pdu_primitives.P_DATA,
]


class Event(pynetdicom.events.Event):
    event: EventTuple
    primitive: Primitive
    assoc: pynetdicom.association.Association
    dataset: pydicom.Dataset


AssociationQueue = asyncio.Queue[Event]
AssociationQueueStore = dict[str, AssociationQueue]
AeQueue = asyncio.Queue[AssociationQueue]


async def start_pynetdicom_server(
    ae: pynetdicom.ae.ApplicationEntity,
    port: int,
    ae_queue: AeQueue,
    executor: concurrent.futures.ThreadPoolExecutor,
    loop: asyncio.AbstractEventLoop,
):
    def start_server():
        handlers = _build_handlers(ae_queue, loop)

        logging.info("Starting pynetdicom server")
        ae.start_server(("", port), block=True, evt_handlers=handlers)

    loop.run_in_executor(executor, start_server)


def build_ae():
    ae = pynetdicom.AE()

    ae.network_timeout = None
    ae.acse_timeout = None
    ae.dimse_timeout = None
    ae.maximum_pdu_size = 0

    storage_sop_classes = [
        cx.abstract_syntax for cx in pynetdicom.AllStoragePresentationContexts
    ]
    verification_sop_classes = [
        cx.abstract_syntax for cx in pynetdicom.VerificationPresentationContexts
    ]
    sop_classes = storage_sop_classes + verification_sop_classes

    for uid in sop_classes:
        ae.add_supported_context(uid, pynetdicom.ALL_TRANSFER_SYNTAXES)

    return ae


def _build_handlers(ae_queue: AeQueue, loop: asyncio.AbstractEventLoop):
    association_queue_store: AssociationQueueStore = dict()

    handlers = [
        (
            pynetdicom.evt.EVT_ESTABLISHED,
            _established_handler,
            [association_queue_store, ae_queue, loop],
        ),
        (pynetdicom.evt.EVT_C_STORE, _queue_handler, [association_queue_store, loop]),
        (
            pynetdicom.evt.EVT_ACSE_RECV,
            _blocking_queue_handler,
            [association_queue_store, loop],
        ),
    ]

    return handlers


def _established_handler(
    event: Event,
    association_queue_store: AssociationQueueStore,
    ae_queue: AeQueue,
    loop: asyncio.AbstractEventLoop,
):
    future = asyncio.run_coroutine_threadsafe(
        _create_new_association_queue(ae_queue), loop
    )
    association_queue = future.result()
    association_queue_store[event.assoc.name] = association_queue

    return 0x0000


async def _create_new_association_queue(ae_queue: AeQueue):
    association_queue: AssociationQueue = asyncio.Queue()
    asyncio.create_task(ae_queue.put(association_queue))

    return association_queue


def _queue_handler(
    event: Event,
    association_queue_store: AssociationQueueStore,
    loop: asyncio.AbstractEventLoop,
):
    queue = association_queue_store[event.assoc.name]
    asyncio.run_coroutine_threadsafe(_add_to_queue(queue, event), loop)

    return 0x0000


async def _add_to_queue(association_queue: AssociationQueue, event: Event):
    await association_queue.put(event)


def _blocking_queue_handler(
    event: Event,
    association_queue_store: AssociationQueueStore,
    loop: asyncio.AbstractEventLoop,
):
    if isinstance(event.primitive, pynetdicom.pdu_primitives.A_RELEASE):
        queue = association_queue_store[event.assoc.name]
        future = asyncio.run_coroutine_threadsafe(
            _add_to_queue_wait_for_complete(queue, event), loop
        )
        future.result()

    if isinstance(
        event.primitive,
        (
            pynetdicom.pdu_primitives.A_RELEASE,
            pynetdicom.pdu_primitives.A_ABORT,
            pynetdicom.pdu_primitives.A_P_ABORT,
        ),
    ):
        try:
            del association_queue_store[event.assoc.name]
        except KeyError:
            pass

    return 0x0000


async def _add_to_queue_wait_for_complete(
    association_queue: AssociationQueue, event: Event
):
    await association_queue.put(event)
    await association_queue.join()
