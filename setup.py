from distutils.core import setup

setup(name='GoatD3MWrapper',
    version='1.0.1',
    description='A geocoding service from New Knowledge',
    packages=['GoatD3MWrapper'],
    install_requires=["requests","typing"],
    entry_points = {
        'd3m.primitives': [
            'distil.Goat.forward = GoatD3MWrapper:goat',
            'distil.Goat.reverse = GoatD3MWrapper:reverse_goat'
        ],
    },
)
