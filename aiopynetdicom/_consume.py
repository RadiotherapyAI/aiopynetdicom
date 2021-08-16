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
import logging

import pydicom
import pynetdicom
import pynetdicom.pdu_primitives

from . import _ae

Event = _ae.Event

DicomStore = list[pydicom.Dataset]


async def consume(ae_queue: _ae.AeQueue):
    while True:
        association_queue = await ae_queue.get()
        asyncio.create_task(_handle_association_queue(association_queue))


async def _handle_association_queue(association_queue: _ae.AssociationQueue):
    dicom_store: DicomStore = list()

    association_closed = False

    while not association_closed:
        event = await association_queue.get()
        name = event.event.name
        description = event.event.description
        logging.info(f"{name} | {description}")

        if event.event == pynetdicom.evt.EVT_C_STORE:
            await _c_store_handler(event, dicom_store)

        if event.event == pynetdicom.evt.EVT_ACSE_RECV:
            association_closed = await _acse_received_handler(event, dicom_store)

        association_queue.task_done()


async def _c_store_handler(event: Event, dicom_store: DicomStore):
    dicom_store.append(event.dataset)

    logging.info("C Store handler complete")


async def _acse_received_handler(
    event: Event,
    dicom_store: DicomStore,
):
    if isinstance(event.primitive, pynetdicom.pdu_primitives.A_RELEASE):
        logging.info("About to close association")

        instance_uids = [ds.SOPInstanceUID for ds in dicom_store]
        logging.info(f"Found the following instance UIDs: {instance_uids}")

    association_closed = isinstance(
        event.primitive,
        (
            pynetdicom.pdu_primitives.A_RELEASE,
            pynetdicom.pdu_primitives.A_ABORT,
            pynetdicom.pdu_primitives.A_P_ABORT,
        ),
    )

    return association_closed
