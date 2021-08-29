import apache_beam as beam

from base_pipeline import run_pipeline, ShowElementInfo
from apache_beam.io import WriteToPubSub

values = [('A', 1.1), ('A', 1.2), ('A', 1.3), ('A', 1.4), ('A', 1.5), ('A', 1.6), ('A', 1.7)]


def run_fixed_window(pipeline):
    pipeline | beam.Create(values) | ShowElementInfo()


if __name__ == '__main__':
    run_pipeline(run_fixed_window)
