import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms import trigger

from utils import GetElementTimestamp, PrettyPrint
from samples import samples


def simple_stream():
    with beam.Pipeline(options=PipelineOptions(streaming=True)) as p:
        windowed_elements = (
                p | samples.StreamSamples.get_stream_without_timestamped_values()
                | beam.Map(lambda x: x)  # Work around for typing issue
                | beam.WindowInto(beam.window.FixedWindows(1))
        )

        (windowed_elements
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


def stream_with_late_data_and_default_trigger():
    with beam.Pipeline(options=PipelineOptions(streaming=True)) as p:
        windowed_elements = (
                p | samples.StreamSamples.get_late_data_stream()
                | beam.Map(lambda x: x)  # Work around for typing issue
                | beam.WindowInto(beam.window.FixedWindows(2))
        )

        (windowed_elements
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


def stream_with_late_data():
    with beam.Pipeline(options=PipelineOptions(streaming=True)) as p:
        windowed_elements = (
                p | samples.StreamSamples.get_late_data_stream()
                | beam.Map(lambda x: x)  # Work around for typing issue
                | beam.WindowInto(
                     beam.window.GlobalWindows(),
                     trigger=trigger.AfterWatermark(
                         early=trigger.AfterAny(
                             trigger.AfterCount(2),
                             trigger.AfterProcessingTime(3)
                         ),
                         late=trigger.AfterCount(2),
                     ),
                     accumulation_mode=beam.trigger.AccumulationMode.DISCARDING,
                     allowed_lateness=0
                 )
        )

        (windowed_elements
            | "10" >> beam.ParDo(GetElementTimestamp())
            | "11" >> PrettyPrint()
        )

        (
            windowed_elements
            | beam.combiners.Count.PerKey()
            | beam.Map(lambda x: f'Count is {x[1]}')
            | beam.ParDo(GetElementTimestamp(print_pane_info=True))
            | PrettyPrint()
        )


def stream_with_late_data_and_early_trigger():
    with beam.Pipeline(options=PipelineOptions(streaming=True)) as p:
        windowed_elements = (
                p | samples.StreamSamples.get_late_data_stream()
                # | beam.Map(lambda x: x)  # Work around for typing issue
                # | beam.WindowInto(beam.window.FixedWindows(5),
                #                   trigger=beam.trigger.AfterWatermark(early=beam.trigger.AfterCount(3)),
                #                   accumulation_mode=beam.trigger.AccumulationMode.ACCUMULATING,
                #                   allowed_lateness=0)
                | beam.WindowInto(beam.window.GlobalWindows(),
                               trigger=beam.trigger.AfterWatermark(
                                   early=beam.trigger.AfterAny(
                                       beam.trigger.AfterCount(2),
                                       # beam.trigger.AfterProcessingTime(2)
                                       ),
                               ),
                              accumulation_mode=beam.trigger.AccumulationMode.DISCARDING,
                              allowed_lateness=0)
        )

        (windowed_elements
            | "10" >> beam.ParDo(GetElementTimestamp())
            | "11" >> PrettyPrint()
        )

        (
            windowed_elements
            | beam.combiners.Count.PerKey()
            | beam.Map(lambda x: f'Count is {x[1]}')
            | beam.ParDo(GetElementTimestamp(print_pane_info=True))
            | PrettyPrint()
        )


if __name__ == '__main__':
    # simple_stream()
    # stream_with_late_data_and_default_trigger()
    stream_with_late_data()
    # stream_with_late_data_and_early_trigger()