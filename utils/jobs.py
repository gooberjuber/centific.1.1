from databricks_api import DatabricksAPI
from typing import Set, Dict
from . import need

# all_jobs returns a list of jobs associated to the account
# pass the db object and a set of strings called needs, the jobs returned will have the passed 'needs' attributes only
# if all attributes are needed just pass empty set
def all_jobs(db: DatabricksAPI, needs: Set[str] = set()) -> Dict[str, any]:
    try:
        jobs = db.jobs.list_jobs()['jobs']
        return {"status": True, "data": need.need_only(needs, jobs)}
    except Exception as e:
        print("*" * 10 + f"clusters.py -> list_clusters E : {e}")
        return {'status': False, "data": str(e)}