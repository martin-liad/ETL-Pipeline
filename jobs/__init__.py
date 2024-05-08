from pathlib import Path

class GlobalEnv(object):
    """
    Holds configuration variables for ETL jobs.
    """
    path_raw: Path
    path_downloaded: Path
    path_interim: Path
    path_processed: Path

    def __init__(self, **kwargs):
        # Init properties from varargs
        self.__dict__.update(kwargs)

class JobEnv(object):
    """
    Parent class for ETL jobs.
    """

    def __init__(self, global_env: GlobalEnv, source_name: str, collection_name: str):
        self.global_env = global_env
        self.source_name = source_name
        self.collection_name = collection_name
    
    def raw_path(self):
        return self.global_env.path_raw / self.source_name / self.collection_name

    def downloaded_path(self):
        return self.global_env.path_downloaded / self.source_name / self.collection_name

    def interim_path(self):
        return self.global_env.path_interim / self.source_name / self.collection_name

    def processed_path(self):
        return self.global_env.path_processed / self.source_name / self.collection_name
