import site
import sys


site.addsitedir(r"D:\LantuConnect\ask_data\.pydeps310")
sys.path.insert(0, r"D:\LantuConnect\ask_data\src")

import uvicorn


if __name__ == "__main__":
    uvicorn.run("ndea.main:http_app", host="0.0.0.0", port=8001)
