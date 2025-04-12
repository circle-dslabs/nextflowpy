from nextflowpy.process_engine import process

@process()
def FQ_LINT(meta_fastq, args=""):
    meta, fastq_file = meta_fastq
    sample_id = meta.get("id")
    if not sample_id:
        raise ValueError("FQ_LINT requires meta['id'] to be set.")

    output = f"{sample_id}.fq_lint.txt"

    script = f"""
    fq lint \\
        {args} \\
        {fastq_file} > {output}
    """

    return output

