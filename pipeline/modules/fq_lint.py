from nextflowpy.process_engine import process
from nextflowpy.utils.staged_path import path

@process()
def FQ_LINT(meta_fastq, args="--quiet"):
    meta, fastq_file = meta_fastq
    output_file = f"{meta['id']}.fq_lint.txt"

    script = f"""
    echo linting {fastq_file} with args: {args}
    echo "linted {fastq_file}" > {output_file}
    """

    return (meta, path(output_file)), script
