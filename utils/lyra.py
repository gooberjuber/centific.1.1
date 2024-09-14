import fastapi
from fastapi import FastAPI, HTTPException
from openai import OpenAI
import mongo
import json
from clusters import list_clusters
from workspace import all_files, create_book_path
from jobs import create_job



from databricks_api import DatabricksAPI



db = DatabricksAPI(
    host=DATABRICKS_INSTANCE,
    token=DATABRICKS_TOKEN
)





def heyGPT(message, thread_id, role = "user"):
    message = openAiClient.beta.threads.messages.create(
    thread_id=thread_id,
    role=role,
    content=message
    )

    run = openAiClient.beta.threads.runs.create_and_poll(
    thread_id=thread_id,
    assistant_id=assistantId
    )

    while run.status not in ('requires_action', 'completed'):
        pass

    return run

def GPTsideKick(gpt_response, thread_id):
    if gpt_response.status == 'requires_action':

        intendedCalls = [tool for tool  in gpt_response.required_action.submit_tool_outputs.tool_calls]

        if len(intendedCalls) > 1: print("*" * 10, "WARNING MULTIPLE FNs BEING CALLED BY GPT ONLY FIRST WILL BE ACTUALLY CALLED", "*" * 10)
        intention = intendedCalls[0]

        if intention.function.name == "list_clusters" :
            fn_response = list_clusters(db, set(json.loads(intention.function.arguments)['needs']))
            status, data = fn_response['status'], fn_response['data']
            if not status:
                print("*" * 10, "WARNING", "*" * 10 ,"could not list clusters")

        elif intention.function.name == "all_files":
            fn_response =  all_files(db, path=json.loads(intention.function.arguments)['path'])
            status, data = fn_response['status'], fn_response['data']
            if not status: print("*" * 10, "WARNING", "*" * 10 ,"could not insert challenge to DB")
        
        elif intention.function.name == "create_book_path":
            fn_response =  create_book_path(db, workspace_path=json.loads(intention.function.arguments)['workspace_path'], upload_path=json.loads(intention.function.arguments)['upload_path'])
            status, data = fn_response['status'], fn_response['data']
            if not status: print("*" * 10, "WARNING", "*" * 10 ,"could not insert challenge to DB")
        
        elif intention.function.name == "create_job":
            fn_response =  create_job(db, dependents=json.loads(intention.function.arguments)['dependents'], cluster_ids=json.loads(intention.function.arguments)['cluster_ids'], paths=json.loads(intention.function.arguments)['paths'], job_name=json.loads(intention.function.arguments)['job_name'], task_names=json.loads(intention.function.arguments)['task_names'])
            status, data = fn_response['status'], fn_response['data']
            if not status: print("*" * 10, "WARNING", "*" * 10 ,"could not insert challenge to DB")

        return {"status" : True, "data" : data}
   
    elif gpt_response.status == "completed":
        messages = openAiClient.beta.threads.messages.list(
            thread_id=thread_id
        )
        return {"status" : True, "data" : messages.data[0].content[0].text.value}
    




def getaThread():
    try:
        thread = openAiClient.beta.threads.create()
        return thread.id
    except Exception as e:
        print("thread error")
        return "error"


def messageGPT(message : str, thread_id : str):
    gptResponse = heyGPT(message=message, thread_id=thread_id)
    gptResponse = GPTsideKick(gptResponse, thread_id)
    return gptResponse




t = getaThread()
print(t)

print(messageGPT("hi", t))
print(messageGPT("create me a job sweet hear", t))
print(messageGPT("want a job with 2 tasks say tosk1 and tosk2 and then the notbook path for both tasks is the same that is : /Workspace/Users/gooberjuber@outlook.com/code_multicelltest.ipynb and the clusters for both of them are 0911-051026-t7k7800r and tosk2 depedns on tosk1, tosk1 dont depend on anyone then forgot call the job lyra_child and dont ask me for confirmation just do it", t))