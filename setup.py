from setuptools import setup, find_packages

setup(
    name='sh_scrapy',
    version='0.4',
    packages=find_packages(),
    install_requires=[
        'Scrapy>=0.24',
        'hubstorage>=0.16',
    ],
    entry_points={
        'console_scripts': [
            'start-crawl = sh_scrapy.crawl:main',
        ],
    },
)
