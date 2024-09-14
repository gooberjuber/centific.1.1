import json
import base64
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

# reads a ipynb and returns the JSON format (not exact json just a py dict, but parseable.. i think)
def read_ipynb(local_path: str) -> dict:
    with open(local_path, "r", encoding="utf-8") as f:
        notebook_content = json.load(f)
    return notebook_content

# create's a notebook when passed the JSON(dict) content of a notbook along with a path you want the notebook to be saved to
# Use this alongside read_ipynb() if in case you wanna uplpoad notebook from path directly
def create_notebook(db: DatabricksAPI, path: str, content: dict) -> Dict[str, any]:
    try:
        encoded_content = base64.b64encode(json.dumps(content).encode('utf-8')).decode('utf-8')
        response = db.workspace.import_workspace(
            path=path,
            #change3d format from source to jupyter
            format='JUPYTER',
            language='PYTHON',
            content=encoded_content
        )
        return {'status': True, "data": response}
    except Exception as e:
        print("workspace.py -> create_notebook() -> E", e)
        return {'status': False, "data": str(e)}

# This function is created specifically for the assistant to pass path directly this combines the above two fucntions

def create_book_path(db : DatabricksAPI, upload_path : str, workspace_path : str):
    content = read_ipynb(upload_path)
    return create_notebook(db, workspace_path, content)

