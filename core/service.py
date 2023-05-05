import os
from pathlib import Path

import core.integrations
import core.crosscutting
import core.jobs


def init_app():
    is_gunicorn = "gunicorn" in os.environ.get("SERVER_SOFTWARE", "")
    print('is_gunicorn: %d' % is_gunicorn)

    Path("data/api").mkdir(parents=True, exist_ok=True)
    refresh_data()
    core.jobs.start_jobs()


def refresh_data():
    core.integrations.refresh_data()
