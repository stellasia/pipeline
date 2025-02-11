import uvicorn
from fastapi import FastAPI
from fastapi.responses import StreamingResponse

from pipeline import run_pipeline_step_by_step

app = FastAPI()


@app.get("/")
async def run_pipeline(show_task_progress: bool = False):
    """Stream response from the run_pipeline_step_by_step generator"""
    return StreamingResponse(run_pipeline_step_by_step(show_task_progress))


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
