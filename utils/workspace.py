from databricks_api import DatabricksAPI
from typing import List, Dict

# all files return a list of strings where each is a path to a file
# the files across dirs are traversed recursively
def all_files(db: DatabricksAPI, path: str = "/") -> Dict[str, any]:
    try:
        items = db.workspace.list(path)
        files = []
        for item in items.get("objects", []):
            if item["object_type"] == "DIRECTORY":
                # Recursively list files in subdirectories
                files.extend(all_files(db, item["path"])["data"])
            else:
                files.append(item["path"])
        return {"status": True, "data": files}
    except Exception as e:
        print("*" * 10 + " workspace.py -> all_files -> E : ", e)
        return {"status": False, "data": str(e)}