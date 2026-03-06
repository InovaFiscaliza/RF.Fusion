# modules/task/builder.py

NONE_FILTER = {
    "mode": "NONE",
    "start_date": None,
    "end_date": None,
    "last_n_files": None,
    "extension": None,
    "file_path": "/mnt/internal/data",
    "file_name": None,
    "agent": "local"
}


def build_filter(
    mode: str,
    start_date=None,
    end_date=None,
    last_n_files=None,
    extension=None,
    file_path=None,
    file_name=None,
):
    return {
        "mode": mode,
        "start_date": start_date,
        "end_date": end_date,
        "last_n_files": last_n_files,
        "extension": extension,
        "file_path": file_path,
        "file_name": file_name,
        "agent": "local",
    }