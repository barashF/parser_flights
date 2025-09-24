from fastapi import FastAPI, File, UploadFile
import pandas as pd

from application.utils.parser import parse_file


app = FastAPI(docs_url="/api/swagger")

@app.post("/upload/")
async def upload_excel(file: UploadFile = File(...)):
    contents = await file.read()
    parse_file(contents, file.filename)