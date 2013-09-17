from setuptools import setup, find_packages
setup(
    name = "cftp",
    version = "0.1",
    packages = find_packages(),
    entry_points = {
        'console_scripts': [
            'cftp = cftp.shell:main',
        ]
    }
)
