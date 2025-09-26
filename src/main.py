import uvicorn

from application.app import create_app
from configuration.config import get_settings



config = get_settings()


if __name__ == "__main__":
    uvicorn.run(create_app(), host=config.app.host, port=config.app.port)