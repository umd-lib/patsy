def pytest_addoption(parser):
    parser.addoption(
        '--base-url',
        action='store',
        default=':memory:',
        help='Base Database URL for the tests'
    )
