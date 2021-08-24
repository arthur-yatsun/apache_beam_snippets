import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

from utils import GetElementTimestamp, PrettyPrint
from samples import samples

if __name__ == '__main__':
    global_window = beam.transforms.window.GlobalWindow()
    print(f"{global_window.start=}, {global_window.end=}")

    with beam.Pipeline(options=PipelineOptions()) as p:
        (
            p | beam.Create([1, 2, 3])
            | beam.ParDo(GetElementTimestamp())
            | PrettyPrint()
        )

    for element in samples.BatchSamples.get_timestamped_values():
        print(f"Value {element.value} has event timestamp "
              f"{element.timestamp.to_utc_datetime()}")

