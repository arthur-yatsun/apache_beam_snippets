import apache_beam as beam

from base_pipeline import DirectRunner


def experiments(pipeline):
    global_window = beam.transforms.window.GlobalWindow()
    print(f'{global_window.start=}, {global_window.end=}')


if __name__ == '__main__':
    DirectRunner.run(experiments)
