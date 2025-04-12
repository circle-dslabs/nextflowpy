from nextflowpy.process_engine import process

@process()
def FQ_LINT(meta_fastq, args=""):
    meta, fastq_file = meta_fastq
    sample_id = meta["id"]
    output = f"{sample_id}.fq_lint.txt"

    script = f"""
    echo linting {fastq_file} > {output}
    """

    return output, script
