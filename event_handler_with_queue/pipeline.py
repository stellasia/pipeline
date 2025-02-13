from neo4j_graphrag.experimental.pipeline import Pipeline, Component, DataModel


class MyComponentOutput(DataModel):
    output: str


class MyComponent(Component):

    async def run(self, input: str) -> MyComponentOutput:
        return MyComponentOutput(output=input)


def get_pipeline(callback):
    pipeline = Pipeline(callback=callback)
    pipeline.add_component(MyComponent(), "component")
    return pipeline
