import os
import subprocess
import hashlib
import shutil
import logging
from typing import Callable, List, Union, Any
from nextflowpy.logger import logger

registered_processes = []
_workflows = []

# Global params dictionary (can be overridden in main workflow)
params = {
    "workDir": ".nextflowpy/work",
    "publishDir": "results"
}

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
        logger.info(f"🔍 [{self.name}] Input: {input_value}")

        result = self.func(input_value, **kwargs)

        if not isinstance(result, tuple) or len(result) != 2:
            raise ValueError(f"[{self.name}] must return a tuple: (output, script)")

        output, script = result

        work_hash = hashlib.md5((self.name + str(input_value)).encode()).hexdigest()
        work_dir = os.path.join(params.get("workDir", ".nextflowpy/work"), work_hash)
        os.makedirs(work_dir, exist_ok=True)
        logger.info(f"📂 Workdir: {work_dir}")

        script_path = os.path.join(work_dir, "script.sh")
        with open(script_path, "w") as f:
            f.write(script)

        logger.info(f"📝 Script:\n{script.strip()}")
        logger.info(f"📤 Output expected: {output}")

        try:
            subprocess.run("bash script.sh", shell=True, check=True, cwd=work_dir)
        except subprocess.CalledProcessError as e:
            logger.error(f"❌ Script failed in {work_dir}: {e}")
            return None

        final_output = os.path.join(work_dir, os.path.basename(output))
        if os.path.exists(final_output):
            logger.info(f"✅ Output found: {final_output}")

            publish_dir = params.get("publishDir")
            if publish_dir:
                os.makedirs(publish_dir, exist_ok=True)
                destination = os.path.join(publish_dir, os.path.basename(output))
                shutil.copy(final_output, destination)
                logger.info(f"📦 Published to: {destination}")
        else:
            logger.warning(f"⚠️ Output missing: {final_output}")

        return final_output

def process(*, parallel: bool = True):
    def wrapper(func: Callable):
        return ProcessWrapper(func, parallel=parallel)
    return wrapper

def workflow(func: Callable):
    _workflows.append(func.__name__)
    def wrapper():
        logger.info(f"🚀 Starting workflow: {func.__name__}")
        return func()
    return wrapper
