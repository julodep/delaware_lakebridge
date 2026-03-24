from databricks.sdk import WorkspaceClient
import os

w = WorkspaceClient()

file_path = r"C:\Users\depoplimontj\.databricks\labs\lakebridge_profilers\synapse_assessment\profiler_extract.db"

with open(file_path, "rb") as f:
    w.files.upload("/Volumes/dbe_dbx_internships/julien-internship/lakebridge-volume/profiler_extract.db", f, overwrite=True)

print(f"Upload succeeded: {os.path.getsize(file_path) / 1024 / 1024:.1f} MB")