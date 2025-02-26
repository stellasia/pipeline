from typing import List, Dict, Any
import asyncio
from dataclasses import dataclass
from contextlib import asynccontextmanager


@dataclass
class ComponentResult:
    result: Any
    event: asyncio.Event


class Pipeline:
    def __init__(self, components: List[Dict[str, Any]]):
        self.components = components
        self._status_queue = asyncio.Queue()
        self._results: Dict[str, ComponentResult] = {}

    async def __aiter__(self):
        while True:
            try:
                status = await self._status_queue.get()
                if status['status'] == 'completed':
                    yield status
                    break
                yield status
            except asyncio.CancelledError:
                break

    @asynccontextmanager
    async def _component_error_handler(self, component_name: str):
        try:
            yield
        except Exception as e:
            await self._status_queue.put({
                'status': 'component_error',
                'data': {
                    'component': component_name,
                    'error': getattr(e, 'message', f"{e.__class__.__name__}: {str(e)}")
                }
            })
            return

    async def _run_component(self, component: Dict[str, Any], input: Any):
        async with self._component_error_handler(component['name']):
            await self._status_queue.put({
                'status': 'component_started',
                'data': {'component': component['name']}
            })

            # Initialize result holder with event
            self._results[component['name']] = ComponentResult(
                result=None,
                event=asyncio.Event()
            )

            # Wait for dependencies
            dep_results = {}
            for dep in component.get('dependencies', []):
                if dep not in self._results:
                    self._results[dep] = ComponentResult(
                        result=None,
                        event=asyncio.Event()
                    )
                await asyncio.wait_for(self._results[dep].event.wait(), timeout=3)
                dep_results[dep] = self._results[dep].result

            # Run the component
            result = await asyncio.wait_for(component['func'](input, dep_results), timeout=3)

            # Store result and signal completion
            self._results[component['name']].result = result
            self._results[component['name']].event.set()

            await self._status_queue.put({
                'status': 'component_completed',
                'data': {
                    'component': component['name'],
                    'result': result
                }
            })

            return result

    async def run(self, input: Any):
        async with self._component_error_handler('pipeline'):
            await self._status_queue.put({
                'status': 'started',
                'data': {'input': input}
            })

            tasks = [
                asyncio.create_task(
                    self._run_component(component, input),
                    name=f"pipeline_component_{component['name']}"
                )
                for component in self.components
            ]

            await asyncio.gather(*tasks)

            final_results = {
                comp['name']: self._results[comp['name']].result
                for comp in self.components
                if not comp.get('dependencies')
            }

            await self._status_queue.put({
                'status': 'completed',
                'data': {'final_results': final_results}
            })

            return final_results


# Example usage showing different parallel patterns:
async def fetch_data(input, deps):
    await asyncio.sleep(2)  # Simulate API call
    return ["data1", "data2", "data3"]


async def process_data(input, deps):
    await asyncio.sleep(1)  # Simulate processing
    return {"processed": True}


async def enrich_data(input, deps):
    data = deps['fetch_data']
    processing = deps['process_data']
    await asyncio.sleep(1)
    return {
        "enriched_data": data,
        "processing_status": processing
    }


async def generate_report(input, deps):
    # raise Exception("Not implemented")
    enriched = deps['enrich_data']
    await asyncio.sleep(0.5)
    return f"Report based on {len(enriched['enriched_data'])} items"


components = [
    {
        'name': 'fetch_data',
        'func': fetch_data,
        'dependencies': []
    },
    {
        'name': 'process_data',
        'func': process_data,
        'dependencies': []
    },
    {
        'name': 'enrich_data',
        'func': enrich_data,
        'dependencies': ['fetch_data', 'process_data']
    },
    {
        'name': 'generate_report',
        'func': generate_report,
        'dependencies': ['enrich_data']
    }
]


async def run_pipeline():
    pipeline = Pipeline(components)
    asyncio.create_task(pipeline.run(input={"x": 12}))
    async for result in pipeline:
        print(result)


if __name__ == '__main__':
    asyncio.run(run_pipeline())
