from distutils.core import setup

setup(name='GoatThinClient',
    version='1.0',
    description='A thin client for interacting with geocoding microservice from New Knowledge, i.e., Goat',
    packages=['GoatThinClient'],
    install_requires=["requests","typing"],
    entry_points = {
        'd3m.primitives': [
            'distil.Goat.forward = GoatThinClient:goat',
            'distil.Goat.reverse = GoatThinClient:reverse_goat'
        ],
    },
)
