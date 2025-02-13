import asyncio
import httpx
import json


async def main(show_task_progress: bool = True):
    """Consumes the streaming response from the server.
    """
    client = httpx.AsyncClient()
    async with client.stream('GET', 'http://localhost:8000/', params={
        "show_task_progress": show_task_progress,
        "input_text": "here is my question: ...",
    }) as response:
        async for chunk in response.aiter_bytes():
            # print(chunk)
            print(json.loads(chunk.decode()))


if __name__ == '__main__':
    show_task_progress = True
    asyncio.run(main(show_task_progress))

"""
logs:
$ python client.py
{'type': 'task_checkpoint', 'task_name': 'task1', 'result': {'type': 'intermediate', 'iteration': 0, 'num_iterations': 2, 'sub_data': [0, 1, 2, 3, 4]}}
{'type': 'task_checkpoint', 'task_name': 'task1', 'result': {'type': 'intermediate', 'iteration': 1, 'num_iterations': 2, 'sub_data': [5, 6, 7, 8, 9]}}
{'type': 'step', 'task_name': 'task1', 'result': {'type': 'final', 'result': [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]}}
{'type': 'task_checkpoint', 'task_name': 'task2', 'result': {'type': 'intermediate', 'iteration': 0, 'num_iterations': 1, 'sub_data': [36]}}
{'type': 'step', 'task_name': 'task2', 'result': {'type': 'final', 'result': [36]}}
{'type': 'final', 'result': {'task1': {'type': 'final', 'result': [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]}, 'task2': {'type': 'final', 'result': [36]}}}
"""