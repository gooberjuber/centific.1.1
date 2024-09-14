import streamlit as st
import databricks_api
import os
from app import bricks_auth
from app import workspace
from app import jobs
from app import clusters

def authenticate():
    """Function to handle Databricks authentication."""
    if 'authenticated' in st.session_state and st.session_state.authenticated:
        return True

    st.sidebar.title("Login")
    host = st.sidebar.text_input("Host")
    token = st.sidebar.text_input("Token", type="password")

    if st.sidebar.button("Login"):
        if host and token:
            db_object = bricks_auth.bricks_object(host=host, token=token)
            if bricks_auth.verify(db_object):
                st.session_state.authenticated = True
                st.session_state.db = db_object
                st.sidebar.success("Login successful!")
                    
                if 'workspace_files' not in st.session_state:
                    st.session_state.workspace_files = []
                    st.session_state.workspace_files = fetch_workspace()
                
                if 'cluster_ids' not in st.session_state:
                    st.session_state.cluster_ids = []
                    st.session_state.cluster_ids = fetch_clusters()

                if 'existing_jobs' not in st.session_state:
                    st.session_state.existing_jobs = []
                    st.session_state.existing_jobs = fetch_existing_jobs()

                return True
            else:
                st.sidebar.error("Invalid Credentials.")
                return False
        else:
            st.sidebar.error("Please provide both host and token.")
            return False
        
def fetch_workspace():
    workspace_files = workspace.all_files(db=st.session_state.db, path="/")
    if workspace_files["status"]:
        return workspace_files["data"]
    else:
        return []
    
def fetch_clusters():
    clusters_list = clusters.list_clusters(db=st.session_state.db, needs=set(["cluster_id", "cluster_name"]))
    if clusters_list["status"]:
        return clusters_list["data"]
    else:
        return []

def fetch_existing_jobs():
    existing_jobs = jobs.all_jobs(db=st.session_state.db, needs=set(["job_id", "settings"]))
    if existing_jobs["status"]:
        return existing_jobs["data"]
    else:
        return []

def workspace_screen():
    """Displays the Workspace screen and handles file upload to Databricks."""
    st.title("Workspace")

    # Fetch and display workspace files
    files = st.session_state.workspace_files
    if len(files) > 0:
        st.header("Workspace Files")
        for file in files:
            st.write(file)
    else:
        st.error("Failed to retrieve workspace files.")

    st.header("Upload File")

    # File upload functionality
    uploaded_file = st.file_uploader("Upload a file")
    
    if uploaded_file is not None:
        try:
            # Save the uploaded file to a local directory
            local_path = os.path.join("uploads", uploaded_file.name)
            with open(local_path, "wb") as f:
                f.write(uploaded_file.getbuffer())

            # After file is uploaded, ask for the workspace path
            upload_path = st.text_input("Enter the path to upload the file in Databricks workspace")

            if upload_path:
                # Upload the file to the Databricks workspace
                contents = workspace.read_ipynb(local_path=local_path)
                uploaded = workspace.create_notebook(db=st.session_state.db, path=upload_path, content=contents)

                if uploaded["status"]:
                    st.success(f"File successfully uploaded to Databricks workspace at: {upload_path}")
                    st.session_state.workspace_files.append(upload_path)
                else:
                    st.error("Failed to upload file to Databricks workspace.")
                
                # Cleanup: Remove the local file after it has been uploaded to the workspace
                os.remove(local_path)
        except Exception as e:
            st.error(f"An error occurred during file processing: {e}")
    else:
        st.info("Please upload a file to proceed.")

def jobs_screen():
    """Displays the Jobs screen with dynamic 'Add' functionality."""
    st.title("Jobs")
    if "selected_task" not in st.session_state:
        st.session_state.selected_task = None

    show_existing_tasks = st.checkbox("Show Existing Jobs")

    if show_existing_tasks:
        ids = []
        jobs_list = []
        for i in st.session_state.existing_jobs:
            ids.append(i["job_id"])
            jobs_list.append(i["settings"]["name"])

        select_job = st.selectbox("Select existing Jobs", jobs_list)
        if select_job:
            index = jobs_list.index(select_job)
            st.session_state.selected_task = ids[index]
    else:

    # Initialize session state for task details if not already present
        if "task_names" not in st.session_state:
            st.session_state.task_names = []
        if "notebook_paths" not in st.session_state:
            st.session_state.notebook_paths = []
        if "cluster_ids" not in st.session_state:
            st.session_state.cluster_ids = fetch_clusters()  # Fetch cluster IDs dynamically
        if "cluster_ids_form" not in st.session_state:
            st.session_state.cluster_ids_form = []  # Use this for form-specific cluster IDs
        if "dependencies" not in st.session_state:
            st.session_state.dependencies = []


        # Job name input
        job_name = st.text_input("Enter Job Name")

        st.header("Add a Task")

        # Form to add tasks
        with st.form(key="job_form"):
            task_name = st.text_input("Task Name", placeholder="Enter the task name")
            notebook_path = st.text_input("Notebook Path", placeholder="Enter the notebook path for the task")
            cluster_id = st.selectbox("Enter the cluster ID for the task", options=st.session_state.cluster_ids)
            
            # List previous tasks for dependencies
            previous_tasks = st.session_state.task_names
            drop_down_deps = st.multiselect("Dependencies (Previous Tasks)", options=previous_tasks, default=[])

            # Add Task button inside the form
            if st.form_submit_button("Add Task"):
                if task_name and notebook_path and cluster_id:
                    # Append new task details to the session state lists
                    st.session_state.task_names.append(task_name)
                    st.session_state.notebook_paths.append(notebook_path)
                    st.session_state.cluster_ids_form.append(cluster_id["cluster_id"])
                    st.session_state.dependencies.append(drop_down_deps)
                    
                    # Clear the input fields and update the dependency dropdown
                    st.rerun()
                    
                    st.success(f"Task '{task_name}' added!")
                else:
                    st.error("Please fill all fields before adding the task.")

        # Display added tasks immediately in the order they were added
        if st.session_state.task_names:
            st.header("Tasks Added")
            for i, task in enumerate(st.session_state.task_names):
                st.write(f"**Task {i + 1}:**")
                st.write(f"- **Task Name:** {task}")
                st.write(f"- **Notebook Path:** {st.session_state.notebook_paths[i]}")
                st.write(f"- **Cluster ID:** {st.session_state.cluster_ids_form[i]}")
                st.write(f"- **Dependencies:** {', '.join(st.session_state.dependencies[i]) if st.session_state.dependencies[i] else 'None'}")
                st.write("---")  # Separator between tasks

        create_button = st.button("Create")
        if create_button:
            created = jobs.create_job(
                db=st.session_state.db, 
                job_name=job_name,
                task_names=st.session_state.task_names,
                paths=st.session_state.notebook_paths,
                dependents=st.session_state.dependencies,
                cluster_ids=st.session_state.cluster_ids_form
            )

            if created["status"]:
                st.success(f"Job '{job_name}' has been created successfully!")
                # print(created["data"])
                st.session_state.selected_task = created["data"]["job_id"] #todo
            else:
                st.error("An error occurred while creating the job. Please try again.")
                st.error(f"Error details: {created.get('data', 'No additional details available')}")

    if st.session_state.selected_task is not None:
        run_button = st.button("Run")    
        if run_button:
            runned = jobs.run_job(db=st.session_state.db, job_id=st.session_state.selected_task)
            if runned["status"]:
                st.success(f"Job with run id: {st.session_state.selected_task} has started successfully")
            else:
                st.error("Error occured, Try again.")


def runs_and_status():
    """Displays the Runs and Status screen."""
    st.title("Runs and status")
    st.write("Welcome to Runs and status")

def ai():
    """Displays the AI screen."""
    st.title("AI")
    st.write("Welcome to AI")

def main():
    """Main function handling the navigation and authentication."""
    if authenticate():
        st.sidebar.title("Navigation")
        option = st.sidebar.radio("Choose a screen", ["Workspace", "Jobs", "Runs and status", "AI"])

        if option == "Workspace":
            workspace_screen()
        elif option == "Jobs":
            jobs_screen()
        elif option == "Runs and status":
            runs_and_status()
        elif option == "AI":
            ai()

if __name__ == "__main__":
    # Initialize session state for authentication status
    if 'authenticated' not in st.session_state:
        st.session_state.authenticated = False

    # Create uploads directory if not exists
    if not os.path.exists("uploads"):
        os.makedirs("uploads")

    main()
