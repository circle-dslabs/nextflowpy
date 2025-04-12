from nextflowpy.process_engine import workflow
from pipeline.modules.fq_lint import FQ_LINT
from pipeline.modules.trim_galore import TRIMGALORE

@workflow
def main():
    inputs = [
        ({"id": "sample1", "strandedness": "reverse"}, "data/test/sample1.fastq"),
        ({"id": "sample2", "strandedness": "forward"}, "data/test/sample2.fastq"),
    ]

    linted = FQ_LINT(inputs, args="--quiet")
    trimmed = TRIMGALORE(linted, args="--length 20")

main()
