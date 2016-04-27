from setuptools import setup, find_packages

setup(
    name='scrapinghub-entrypoint-scrapy',
    version='0.5.0',
    packages=find_packages(),
    install_requires=[
        'Scrapy>=1.0',
        'hubstorage>=0.22',
    ],
    entry_points={
        'console_scripts': [
            'start-crawl = sh_scrapy.crawl:main',
        ],
    },
)
