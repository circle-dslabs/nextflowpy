import os
import subprocess
import hashlib
import shutil
import logging
import inspect
from typing import Callable, List, Union, Any
from nextflowpy.logger import logger

registered_processes = []
_workflows = []

class ProcessWrapper:
    def __init__(self, func: Callable, parallel: bool = True):
        self.func = func
        self.name = func.__name__
        self.parallel = parallel
        registered_processes.append(self)

    def __call__(self, input_data: Union[List[Any], Any], **kwargs):
        if self.parallel and isinstance(input_data, list):
            return [self.run_single(item, **kwargs) for item in input_data]
        else:
            return self.run_single(input_data, **kwargs)

    def run_single(self, input_value: Any, **kwargs):
        logger.info(f"Ì¥ç [{self.name}] Input: {input_value}")

        # Call function and get local variables
        frame = {}
        def traced_func():
            output = self.func(input_value, **kwargs)
            frame.update(locals())
            return output

        output = traced_func()
        script = frame.get("script")

        if not script:
            raise ValueError(f"Process {self.name} must define a 'script' variable")

        work_hash = hashlib.md5((self.name + str(input_value)).encode()).hexdigest()
        work_dir = os.path.join(".nextflowpy", "work", work_hash)
        os.makedirs(work_dir, exist_ok=True)
        logger.info(f"Ì≥Ç Workdir: {work_dir}")

        script_path = os.path.join(work_dir, "script.sh")
        with open(script_path, "w") as f:
            f.write(script)

        logger.info(f"Ì≥ù Script:\n{script.strip()}")
        logger.info(f"Ì≥§ Output expected: {output}")

        try:
            subprocess.run("bash script.sh", shell=True, check=True, cwd=work_dir)
        except subprocess.CalledProcessError as e:
            logger.error(f"‚ùå Script failed in {work_dir}: {e}")
            return None

        final_output = os.path.join(work_dir, os.path.basename(output))
        if os.path.exists(final_output):
            logger.info(f"‚úÖ Output found: {final_output}")
        else:
            logger.warning(f"‚ö†Ô∏è Output missing: {final_output}")

        return final_output

def process(*, parallel: bool = True):
    def wrapper(func: Callable):
        return ProcessWrapper(func, parallel=parallel)
    return wrapper

def workflow(func: Callable):
    _workflows.append(func.__name__)
    def wrapper():
        logger.info(f"Ì∫Ä Starting workflow: {func.__name__}")
        return func()
    return wrapper

