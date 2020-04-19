from typing import List

from app.database import db_models, pyd_models, database, submit

from fastapi import FastAPI, Depends, status, BackgroundTasks
from fastapi.routing import Response
from fastapi.middleware.gzip import GZipMiddleware

from sqlalchemy.orm import Session


db_models.Base.metadata.create_all(bind=database.engine)

app = FastAPI(
    title="DatSav",
    description="Submission Point for Distributed Fetcher to Save newly found URLs",
    version="0.0.2",
    redoc_url=None,
)

app.add_middleware(GZipMiddleware, minimum_size=1000)


# Dependency
def get_db():
    try:
        db = database.SessionLocal()
        yield db
    finally:
        db.close()


@app.post(
    "/submit/",
    status_code=status.HTTP_202_ACCEPTED,
    tags=["Submission"],
    summary="Submit URLs",
)
async def submit_frontier(submission: pyd_models.SubmitFrontier,
                          background_tasks: BackgroundTasks,
                          db: Session = Depends(get_db)):
    """
    Submit a List of FQDNs and URLs

    - **crawler_uuid**: Your crawlers UUID
    - **urls_count**: Amount of Url-Elements
    - **urls**: List of Url-Elements
    """

    background_tasks.add_task(submit.save_new_frontier, db, submission)

    return Response(status_code=status.HTTP_202_ACCEPTED)


