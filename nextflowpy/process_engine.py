import os
import subprocess
import hashlib
import shutil
import logging
from typing import Callable, List, Union, Any, Tuple, Dict

from nextflowpy.logger import logger
from nextflowpy.utils.staged_path import StagedPath

# Global config
_global_params = {
    "workDir": "work",
    "publishDir": "results",
    "stageInMode": "copy"  # or 'symlink'
}

def params(new_params: Dict[str, Any]):
    _global_params.update(new_params)

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
        logger.info(f"🔍 [{self.name}] Input: {input_value}")

        result = self.func(input_value, **kwargs)
        if not isinstance(result, tuple) or len(result) != 2:
            raise ValueError(f"[{self.name}] must return (output, script)")

        output, script = result

        # Create unique workdir
        work_hash = hashlib.md5((self.name + str(input_value)).encode()).hexdigest()
        work_base = os.path.abspath(_global_params["workDir"])
        work_dir = os.path.join(work_base, work_hash)

        os.makedirs(work_dir, exist_ok=True)
        logger.info(f"📂 Workdir: {work_dir}")

        # Stage in inputs
        staged_input = self._stage_in(input_value, work_dir)

        # Write .command.sh
        script_path = os.path.join(work_dir, ".command.sh")
        with open(script_path, "w") as f:
            f.write(script)
        os.chmod(script_path, 0o755)

        logger.info(f"📝 Script:\n{script.strip()}")
        logger.info(f"📤 Output expected: {output}")

        # Run script
        try:
            subprocess.run("./.command.sh", shell=True, check=True, cwd=work_dir)
        except subprocess.CalledProcessError as e:
            logger.error(f"❌ Script failed in {work_dir}: {e}")
            return None

        # Stage out outputs and update output to correct workdir path
        output = self._stage_out_and_update(output, work_dir)
        return output

    def _stage_in(self, input_value: Any, work_dir: str):
        stage_mode = _global_params.get("stageInMode", "copy")

        def stage_file(f):
            if isinstance(f, StagedPath):
                src = f.value
                dest = os.path.join(work_dir, os.path.basename(src))
                if not os.path.exists(src):
                    raise FileNotFoundError(f"Cannot stage in: {src}")
                if stage_mode == "copy":
                    shutil.copy(src, dest)
                elif stage_mode == "symlink":
                    os.symlink(os.path.abspath(src), dest)

        def recursive_stage(obj):
            if isinstance(obj, (list, tuple)):
                for item in obj:
                    recursive_stage(item)
            elif isinstance(obj, dict):
                for v in obj.values():
                    recursive_stage(v)
            else:
                stage_file(obj)

        recursive_stage(input_value)

    def _stage_out_and_update(self, output: Any, work_dir: str):
        def handle_output(obj):
            if isinstance(obj, StagedPath):
                fname = os.path.basename(obj.value)
                src_path = os.path.join(work_dir, fname)
                if not os.path.exists(src_path):
                    logger.warning(f"⚠️ Output file missing: {src_path}")
                    return obj
                # Publish to publishDir
                publish_dir = _global_params.get("publishDir", "results")
                os.makedirs(publish_dir, exist_ok=True)
                dest_path = os.path.join(publish_dir, fname)
                shutil.copy(src_path, dest_path)
                logger.info(f"📦 Published to: {dest_path}")
                return StagedPath(src_path)
            elif isinstance(obj, (list, tuple)):
                return type(obj)(handle_output(x) for x in obj)
            elif isinstance(obj, dict):
                return {k: handle_output(v) for k, v in obj.items()}
            else:
                return obj

        return handle_output(output)

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
