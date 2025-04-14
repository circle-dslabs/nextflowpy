from nextflowpy.process_engine import process
from nextflowpy.utils.staged_path import path

@process()
def TRIMGALORE(meta_linted, args="--length 20"):
    meta, linted_file = meta_linted
    output_file = f"{meta['id']}.trimmed.fq.gz"

    script = f"""
    echo trimming {linted_file} with args: {args}
    cp {linted_file} {output_file}
    """

    return (meta, path(output_file)), script
