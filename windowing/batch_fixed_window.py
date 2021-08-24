import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

from utils import GetElementTimestamp, PrettyPrint
from samples import samples

if __name__ == '__main__':
    with beam.Pipeline(options=PipelineOptions()) as p:

        windowed_elements = (
            p | beam.Create(samples.BatchSamples.get_timestamped_values())
            | beam.Map(lambda x: x)  # Work around for typing issue
            | beam.WindowInto(beam.window.FixedWindows(1))
        )

        (
            windowed_elements
            | "10" >> beam.ParDo(GetElementTimestamp())
            | "11" >> PrettyPrint()
        )

        (
            windowed_elements
            | beam.combiners.Count.PerKey()
            | beam.Map(lambda x: f'Count is {x[1]}')
            | beam.ParDo(GetElementTimestamp())
            | PrettyPrint()
        )
