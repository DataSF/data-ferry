from .netfile_client import Form700_Blocking


def run_sync(**kwargs):
    client = Form700_Blocking(**kwargs)
    client.sync()
