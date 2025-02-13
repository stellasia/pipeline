import asyncio
import json

import uvicorn

from queue import Queue

from fastapi import FastAPI
from fastapi.responses import StreamingResponse
from neo4j_graphrag.experimental.pipeline.types import Event, EventType

from event_handler_with_queue.pipeline import get_pipeline


app = FastAPI()


async def stream_progress(queue: Queue):
    """
    Generator function that yields status updates from the queue
    """
    while True:
        # Get status from queue, waiting if necessary
        status_update = await asyncio.get_event_loop().run_in_executor(
            # TODO:
            #   changed queue.get to queue.get_nowait because coroutines
            #   are not supported in this setup, but is it ok to use get_nowait?
            None, queue.get_nowait
        )

        status = status_update.get("status")

        if status == EventType.PIPELINE_FINISHED:
            # Send final update and end stream
            yield json.dumps(status_update, default=str)
            break

        # just for testing the streaming is working
        await asyncio.sleep(1)

        yield json.dumps(status_update, default=str)


@app.get("/")
async def run_pipeline(input_text: str):

    # Create queue for status updates
    status_queue = asyncio.Queue()

    # Define callback that puts status updates into queue
    async def status_callback(event: Event):
        status_update = {
            'status': event.event_type,
            'data': event.model_dump()
        }
        status_queue.put_nowait(status_update)

    # Create pipeline
    pipeline = get_pipeline(status_callback)

    # Start pipeline execution in background
    asyncio.create_task(pipeline.run({"component": {"input": input_text}}))

    # Return streaming response
    return StreamingResponse(
        stream_progress(status_queue),
        media_type='text/event-stream'
    )


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
