import os
import shutil
from nextflowpy.config import get_params

class ContainerExecutor:
    def __init__(self, container_uri: str):
        self.container_uri = container_uri
        self.engine = self._detect_engine()
        self.extra_options = get_params().get("containerOptions", "")
        
        print(f"[ContainerExecutor] Engine: {self.engine} | URI: {self.container_uri}")

    def _detect_engine(self):
        user_specified = get_params().get("container", "auto")
        self.extra_options = get_params().get("containerOptions", "")

        if user_specified != "auto":
            return user_specified

        if self.container_uri.endswith(".sif") or os.path.exists(self.container_uri):
            return "apptainer"

        if shutil.which("apptainer"):
            return "apptainer"
        elif shutil.which("singularity"):
            return "apptainer"
        elif shutil.which("docker"):
            return "docker"
        else:
            raise RuntimeError("No container engine found (apptainer, singularity, docker)")

    def wrap_command(self, command: str, workdir: str):
        if self.engine == "docker":
            image = self.container_uri.replace("docker://", "")
            return f"docker run --rm {self.extra_options} -v {workdir}:{workdir} -w {workdir} {image} bash -c \"{command}\""

        elif self.engine == "apptainer":
            image_path = self.container_uri
            if image_path.startswith("docker://") or image_path.startswith("quay.io") or image_path.startswith("https://"):
                # Auto-pull from registry
                sif_name = os.path.join(workdir, "container.sif")
                pull_cmd = f"apptainer pull {self.extra_options} {sif_name} {self.container_uri}"
                os.system(pull_cmd)
                image_path = sif_name

            return f"apptainer exec {self.extra_options} --pwd {workdir} {image_path} bash -c \"{command}\""

        else:
            raise ValueError(f"Unsupported engine: {self.engine}")

