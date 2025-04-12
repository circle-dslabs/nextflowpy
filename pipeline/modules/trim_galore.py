import os
from nextflowpy.process_engine import process
from nextflowpy.types import path

@process()
def TRIMGALORE(fastq_file, args="--length 20"):
    basename = os.path.basename(str(fastq_file))
    sample_id = basename.split(".")[0]
    output_file = f"{sample_id}.trimmed.fq.gz"

    script = f"""
    echo trimming {basename} with args: {args}
    cp {basename} {output_file}
    """

    return path(output_file), script
