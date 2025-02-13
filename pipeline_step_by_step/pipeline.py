import asyncio
import json
from typing import Any, AsyncGenerator


class Task:
    """Pipeline task"""
    def __init__(self, name: str, batch_size: int = 5, sleep_time: int = 2) -> None:
        self.name = name
        self.batch_size = batch_size
        self.sleep_time = sleep_time

    async def run(self, data: dict[str, Any]) -> Any:
        return data

    async def process_batch(self, data: list[dict[str, Any]], iteration: int) -> dict[str, Any]:
        return {"result": data, "iteration": iteration}

    async def step(self, data: list[dict[str, Any]]) -> AsyncGenerator[Any, None]:
        """For illustration purposes, process batches from input data. Yield result
        for each batch.
        """
        num_iterations = (len(data) - 1) // self.batch_size + 1
        batch_tasks = []
        for iteration in range(num_iterations):
            prev_index = iteration * self.batch_size
            next_index = (iteration + 1) * self.batch_size
            sub_data = data[prev_index:next_index]
            # batch_result = await self.process_batch(sub_data)
            batch_tasks.append(self.process_batch(sub_data, iteration))

        for f in asyncio.as_completed(batch_tasks):
            batch_result = await f
            yield {"type": "intermediate", "num_iterations": num_iterations, **batch_result}
            await asyncio.sleep(self.sleep_time)
        yield {"type": "final", "result": data}


class Pipeline:
    """Pipeline that will process tasks sequentially."""
    def __init__(self, tasks: list[Task]):
        self.tasks = tasks

    async def step(self, data: dict[str, Any], show_task_progress: bool = True) -> AsyncGenerator[dict[str, Any], None]:
        """Run each task and yield its results.
        """
        pipeline_result = {}
        for task in self.tasks:
            input_data = data.get(task.name, [])
            if show_task_progress:
                task_result = None
                async for task_result in task.step(input_data):
                    if task_result["type"] == "intermediate":
                        new_type = "task_checkpoint"
                    else:
                        new_type = "step"
                    yield {"type": new_type, "task_name": task.name, "result": task_result}
            else:
                task_result = await task.run(data)
                yield {"type": "step", "task_name": task.name, "result": task_result}
            pipeline_result[task.name] = task_result
        yield {"type": "final", "result": pipeline_result}

    async def run(self, data: dict[str, Any]) -> dict[str, Any]:
        """Run pipeline end to end, no intermediate yield."""
        r = {}
        async for r in self.step(data):
            # do nothing, just process each step
            ...
        return r


def get_pipeline() -> Pipeline:
    tasks = [
        Task("task1", sleep_time=3),
        Task("task2", sleep_time=1),
    ]
    pipeline = Pipeline(tasks)
    return pipeline


async def run_pipeline_step_by_step(show_task_progress: bool = False) -> AsyncGenerator[str, None]:
    """Return the result yielded by pipeline.step async method until there is no more result.

    NOTE: return type must be str
    """
    pipeline = get_pipeline()
    input_data = {
        "task1": list(range(10)),
        "task2": [36]
    }
    async for x in pipeline.step(input_data, show_task_progress=show_task_progress):
        yield json.dumps(x)


async def run_pipeline() -> None:
    """Old way: run pipeline end to end, no step."""
    pipeline = get_pipeline()
    input_data = {"task1": {"a": 1}, "task2": {"b": 2}}
    res = await pipeline.run(input_data)
    print(res)


if __name__ == "__main__":
    asyncio.run(run_pipeline())
