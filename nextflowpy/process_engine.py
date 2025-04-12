import os
import subprocess
import hashlib
import shutil
import logging
from typing import Callable, List, Union, Any
from nextflowpy.logger import logger
from nextflowpy.types import path

registered_processes = []
_workflows = []

# Global params dictionary
params = {
    "workDir": ".nextflowpy/work",
    "publishDir": "results"
}

def resolve_paths(obj, work_dir):
    if isinstance(obj, path):
        original = obj.value
        staged = os.path.join(work_dir, os.path.basename(original))
        if not os.path.exists(staged):
            os.symlink(os.path.abspath(original), staged)
        return staged
    elif isinstance(obj, (list, tuple)):
        return type(obj)(resolve_paths(x, work_dir) for x in obj)
    elif isinstance(obj, dict):
        return {k: resolve_paths(v, work_dir) for k, v in obj.items()}
    else:
        return obj

class ProcessWrapper:
    def __init__(self, func: Callable, parallel: bool = True):
        self.func = func
        self.name = func.__name__
        self.parallel = parallel
        registered_processes.append(self)

    def __call__(self, input_data: Union[List[Any], Any], **kwargs):
        if self.parallel and isinstance(input_data, list):
            results = [self.run_single(item, **kwargs) for item in input_data]
            return [r for r in results if r is not None]
        else:
            result = self.run_single(input_data, **kwargs)
            return result if result is not None else []

    def run_single(self, input_value: Any, **kwargs):
        logger.info(f"🔍 [{self.name}] Input: {input_value}")

        try:
            result = self.func(input_value, **kwargs)
        except Exception as e:
            logger.error(f"❌ Error in user function: {e}")
            return None

        if not isinstance(result, tuple) or len(result) != 2:
            logger.error(f"❌ [{self.name}] must return a tuple: (output, script)")
            return None

        output, script = result

        work_hash = hashlib.md5((self.name + str(input_value)).encode()).hexdigest()
        work_dir = os.path.join(params.get("workDir", ".nextflowpy/work"), work_hash)
        os.makedirs(work_dir, exist_ok=True)
        logger.info(f"📂 Workdir: {work_dir}")

        resolved_input = resolve_paths(input_value, work_dir)

        frame = {}
        if isinstance(resolved_input, dict):
            frame.update(resolved_input)
        elif isinstance(resolved_input, (list, tuple)):
            frame.update({f"var{i}": v for i, v in enumerate(resolved_input)})
        else:
            frame["input"] = resolved_input

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

        output_items = output if isinstance(output, list) else [output]
        published = []

        for item in output_items:
            if isinstance(item, path):
                file_name = os.path.basename(item.value)
                full_output = os.path.join(work_dir, file_name)

                if os.path.exists(full_output):
                    logger.info(f"✅ Output found: {full_output}")

                    publish_dir = params.get("publishDir")
                    if publish_dir:
                        os.makedirs(publish_dir, exist_ok=True)
                        dest = os.path.join(publish_dir, file_name)
                        shutil.copy(full_output, dest)
                        logger.info(f"📦 Published to: {dest}")
                    published.append(full_output)
                else:
                    logger.warning(f"⚠️ Expected output not found: {full_output}")
            else:
                logger.debug(f"ℹ️ Skipping non-path output: {item}")

        return published if len(published) > 1 else published[0] if published else None

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
