import os
from nextflowpy.process_engine import process

@process()
def TRIMGALORE(fastq_file, args="--length 20"):
    sample_id = os.path.basename(str(fastq_file)).split(".")[0]
    output_file = f"{sample_id}.trimmed.fq.gz"

    script = f"""
    echo trimming {fastq_file} with args: {args}
    cp {fastq_file} {output_file}
    """

    return output_file, script
