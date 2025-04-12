import os
from nextflowpy.process_engine import process

@process()
def FQ_LINT(meta_fastq, args="--quiet"):
    meta, fastq_file = meta_fastq
    sample_id = meta["id"]
    output_file = f"{sample_id}.fq_lint.txt"

    script = f"""
    echo linting {fastq_file} with args: {args}
    echo "linted {fastq_file}" > {output_file}
    """

    return output_file, script
