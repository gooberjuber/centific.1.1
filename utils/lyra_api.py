from fastapi import FastAPI
import lyra

app = FastAPI()

@app.get("/message_lyra")
def message_lyra(message : str, thread_id : str) -> dict:
    return lyra.messageGPT(message, thread_id)

@app.get("/get_thread")
def get_thread():
    return lyra.getaThread()

