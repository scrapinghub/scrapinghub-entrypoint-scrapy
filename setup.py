from setuptools import setup, find_packages

setup(
    name='scrapinghub-entrypoint-scrapy',
    version='0.7.0',
    license='BSD',
    description='Scrapy entrypoint for Scrapinghub job runner',
    long_description=open('README.rst').read(),
    packages=find_packages(),
    install_requires=[
        'Scrapy>=1.0',
        'hubstorage>=0.22',
    ],
    entry_points={
        'console_scripts': [
            'start-crawl = sh_scrapy.crawl:main',
            'list-spiders = sh_scrapy.crawl:list_spiders',
        ],
    },
)
