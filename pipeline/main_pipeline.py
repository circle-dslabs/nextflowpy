from pipeline.modules.fq_lint import FQ_LINT
from pipeline.modules.trim_galore import TRIMGALORE
from nextflowpy.process_engine import workflow, params
from nextflowpy.types import path

params["workDir"] = "work"
params["publishDir"] = "results"

@workflow
def main():
    inputs = [
        ({"id": "sample1", "strandedness": "reverse"}, path("data/test/sample1.fastq")),
        ({"id": "sample2", "strandedness": "forward"}, path("data/test/sample2.fastq"))
    ]

    linted = FQ_LINT(inputs, args="--quiet")
    trimmed = TRIMGALORE([path(p) for p in linted], args="--length 20")


main()
