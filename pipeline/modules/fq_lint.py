from nextflowpy.process_engine import process
from nextflowpy.types import path

@process()
def FQ_LINT(meta_fastq, args=""):
    meta, fastq_file = meta_fastq
    sample_id = meta["id"]
    output_file = f"{sample_id}.fq_lint.txt"

    script = f"""
    echo linting {fastq_file} > {output_file}
    """

    return path(output_file), script