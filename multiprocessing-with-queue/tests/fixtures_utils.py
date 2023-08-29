import os


def fixture_path(filename: str):
    if os.path.exists('fixtures'):
        return os.path.join(os.getcwd(), 'fixtures', filename)

    if os.path.exists(os.path.join('tests', 'fixtures')):
        return os.path.join('tests', 'fixtures', filename)

    raise FileNotFoundError(filename)
