# nextflowpy/config.py

_global_params = {
    "workDir": "work",
    "publishDir": "results",
    "stageInMode": "copy",  # or 'symlink'
    "container": "auto",    # or 'docker', 'apptainer'
    "containerOptions": ""  # extra run options
}

def params(new_params: dict):
    _global_params.update(new_params)

def get_params():
    return _global_params
