from fastapi import APIRouter, HTTPException, Query
from typing import List
import databricks_api
import os
from ..models import models
from ..services import bricks_auth
from ..services import clusters
from ..services import jobs
from ..services import workspace
from utils import lyra
from config import development

app = APIRouter()
db = None

# Authenticates with databricks
# returns true if auth is successful else false
@app.post("/users/authenticate", tags=["users"])
async def authenticate():
    db_object = bricks_auth.bricks_object(development.DATABRICKS_HOST, 
                                            development.DATABRICKS_TOKEN)
    if bricks_auth.verify(db_object):
        global db
        db = databricks_api.DatabricksAPI(
            host=development.DATABRICKS_HOST,
            token=development.DATABRICKS_TOKEN
        )
        print("Successfully authenticated")
        return {"status": True, "data": "Logged in successfully"}
    else:
        raise HTTPException(status_code=401, detail="Invalid credentials")
    

# Gets all the clusters associated with the account
# with optional list of attributes to filter search
@app.get("/clusters/list", tags=["clusters"])
async def get_clusters(needs: List = Query([], description="List of attributes to filter on")):
    if not db:
        print("no db")
        raise HTTPException(status_code=401, detail="Not Authorized")
    
    result = clusters.list_clusters(db, needs=set(needs))
    if not result["status"]:
        raise HTTPException(status_code=500, detail="Error Occured")
    
    return {"status": True, "data": result["data"]}

# Creates a job with mentioned tasks
# returns true if successful else false
@app.post("/jobs/create", tags=["jobs"])
async def create_job(job: models.Job):
    if not db:
        raise HTTPException(status_code=401, detail="Not Authorized")
    
    if not len(job.task_names) == len(job.paths) == len(job.dependents) == len(job.cluster_ids):
        raise HTTPException(status_code=400, detail="Bad request, parameters not same length")
    
    result = jobs.create_job(
        db=db,
        job_name=job.job_name,
        task_names=job.task_names,
        paths=job.paths,
        dependents=job.dependents,
        cluster_ids=job.cluster_ids
    )

    if not result["status"]:
        raise HTTPException(status_code=500, detail="Error Occured")
    
    return {"status": True, "data": result["data"]}

# Gets all the jobs/pipelines associated with the account
# with optional list of attributes to filter search
@app.get("/jobs/list", tags=["jobs"])
async def get_jobs(needs: List = Query([], description="List of attributes to filter on")):
    if not db:
        raise HTTPException(status_code=401, detail="Not Authorized")
    
    result = jobs.all_jobs(db, needs=set(needs))
    if not result["status"]:
        raise HTTPException(status_code=500, detail="Error Occured")
    
    return {"status": True, "data": result["data"]}

# Gets complete metadata of a job
@app.get("/jobs/metadata", tags=["jobs"])
async def get_job(job_id: str, needs: List = Query([], description="List of attributes to filter on")):
    if not db:
        raise HTTPException(status_code=401, detail="Not Authorized")
    
    result = jobs.get_job(db, job_id=job_id, needs=set(needs))

    if not result["status"]:
        raise HTTPException(status_code=500, detail="Error Occured")
    
    return {"status": True, "data": result["data"]}

# Runs a job with valid jab_id
@app.post("/jobs/run", tags=["jobs"])
async def run_job(job_id: str):
    if not db:
        raise HTTPException(status_code=401, detail="Not Authorized")
    
    job = await get_job(job_id=job_id, needs=[])
    if not job["status"]:
        raise HTTPException(status_code=400, detail="Bad request, Job with job_id doesn't exist")
    
    result = jobs.run_job(db, job_id=job_id)

    if not result["status"]:
        raise HTTPException(status_code=500, detail="Error Occured")
    
    return {"status": True, "data": result["data"]}


# Gets information of a specific run using run_id of a job / tasks in job
@app.get("/jobs/run/info", tags=["jobs"])
async def get_job_run_info(run_id: str, needs: List = Query([], description="List of attributes to filter on")):
    if not db:
        raise HTTPException(status_code=401, detail="Not Authorized")
    
    result = jobs.get_run(db, run_id=run_id, needs=set(needs))

    if not result["status"]:
        raise HTTPException(status_code=500, detail="Error Occured")
    
    return {"status": True, "data": result["data"]}

# Gets information of all the runs of a job
@app.get("/jobs/run/allruns", tags=["jobs"])
async def get_job_runs_info(job_id: str, needs: List = Query([], description="List of attributes to filter on")):
    if not db:
        raise HTTPException(status_code=401, detail="Not Authorized")
    
    job = await get_job(job_id=job_id, needs=[])
    if not job["status"]:
        raise HTTPException(status_code=400, detail="Bad request, Job with job_id doesn't exist")

    result = jobs.get_job_runs(db, job_id=job_id, needs=set(needs))

    if not result["status"]:
        raise HTTPException(status_code=500, detail="Error Occured")
    
    return {"status": True, "data": result["data"]}


# Gets all the directories and files in the workspace recursively
# with optional path prefix to filter paths
@app.get("/workspace/list", tags=["workspace"])
async def get_workspace(prefix: str = Query("/", description="Path prefix to filter files")):
    if not db:
        raise HTTPException(status_code=401, detail="Not Authorized")
    
    result = workspace.all_files(db, path=prefix)
    
    if not result["status"]:
        raise HTTPException(status_code=500, detail="Error Occurred")
    
    return {"status": True, "data": result["data"]}

# When given a local path and a file path in the workspace
# Reads contents of the ipynb and create the same on the workspace
# todo: Just use the uploaded file to create the notebook
@app.post("/workspace/upload", tags=["workspace"])
async def upload_file(file: models.Workspace):
    if not db:
        raise HTTPException(status_code=401, detail="Not Authorized")

    if not os.path.exists(file.local_path):
        raise HTTPException(status_code=400, detail="File doesn't exist")

    notebook_as_json = workspace.read_ipynb(local_path=file.local_path)
    result = workspace.create_notebook(db, path=file.upload_path, content=notebook_as_json)

    if not result["status"]:
        raise HTTPException(status_code=500, detail="Error Occurred")
    
    return {"status": True, "data": result["data"]}

@app.get("/message_lyra")
def message_lyra(message : str, thread_id : str) -> dict:
    return lyra.messageGPT(message, thread_id)

@app.get("/get_thread")
def get_thread():
    return lyra.getaThread()