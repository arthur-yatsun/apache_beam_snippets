from typing import Any, Tuple, Iterable

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

from base_pipeline import DirectRunner


global_window = beam.transforms.window.GlobalWindow()
print(f'{global_window.start=}, {global_window.end=}')


def run_pipeline():
    with beam.Pipeline(options=PipelineOptions()) as pipeline:
        pipeline | beam.Create([1, 2, 3]) | beam.ParDo(GetElementInfo())


class GetElementInfo(beam.DoFn):
    """DoFn to get the element info."""

    def process(self, element: Any,
                timestamp=beam.DoFn.TimestampParam,
                **kwargs,
                ) -> Iterable[Any]:

        print(f"{element=}, {timestamp=}")
        yield element


if __name__ == '__main__':
    run_pipeline()
