import os
import subprocess
import hashlib
import shutil
import logging
from typing import Callable, List, Union, Any
from nextflowpy.logger import logger

registered_processes = []
_workflows = []

params = {
    "workDir": ".nextflowpy/work",
    "publishDir": "results"
}

def is_pathlike(value):
    return isinstance(value, str) and (
        os.path.exists(value) or os.path.splitext(value)[1] in [".fastq", ".fq", ".gz", ".txt"]
    )

def stage_inputs(input_obj, work_dir):
    if isinstance(input_obj, str) and is_pathlike(input_obj):
        original_path = os.path.abspath(input_obj)
        staged_path = os.path.join(work_dir, os.path.basename(original_path))
        if not os.path.exists(staged_path):
            if os.path.exists(original_path):
                os.symlink(original_path, staged_path)
        return staged_path
    elif isinstance(input_obj, (list, tuple)):
        return type(input_obj)(stage_inputs(x, work_dir) for x in input_obj)
    elif isinstance(input_obj, dict):
        return {k: stage_inputs(v, work_dir) for k, v in input_obj.items()}
    else:
        return input_obj

def simplify_for_script(obj):
    if isinstance(obj, str) and is_pathlike(obj):
        return os.path.basename(obj)
    elif isinstance(obj, (list, tuple)):
        return type(obj)(simplify_for_script(x) for x in obj)
    elif isinstance(obj, dict):
        return {k: simplify_for_script(v) for k, v in obj.items()}
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
        work_hash = hashlib.md5((self.name + str(input_value)).encode()).hexdigest()
        work_dir = os.path.join(params.get("workDir", ".nextflowpy/work"), work_hash)
        os.makedirs(work_dir, exist_ok=True)
        logger.info(f"📂 Workdir: {work_dir}")

        staged_input = stage_inputs(input_value, work_dir)
        simplified_input = simplify_for_script(staged_input)

        try:
            result = self.func(simplified_input, **kwargs)
        except Exception as e:
            logger.error(f"❌ Error in user function: {e}")
            return None

        if not isinstance(result, tuple) or len(result) != 2:
            logger.error(f"❌ [{self.name}] must return a tuple: (output, script)")
            return None

        output, script = result

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

        published = []
        output_files = output if isinstance(output, list) else [output]

        for item in output_files:
            if isinstance(item, str) and os.path.splitext(item)[1]:
                output_path = os.path.join(work_dir, os.path.basename(item))
                if os.path.exists(output_path):
                    logger.info(f"✅ Output found: {output_path}")
                    publish_dir = params.get("publishDir")
                    if publish_dir:
                        os.makedirs(publish_dir, exist_ok=True)
                        dest = os.path.join(publish_dir, os.path.basename(item))
                        shutil.copy(output_path, dest)
                        logger.info(f"📦 Published to: {dest}")
                    published.append(output_path)
                else:
                    logger.warning(f"⚠️ Expected output not found: {output_path}")

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
