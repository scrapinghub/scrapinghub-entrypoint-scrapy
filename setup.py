from setuptools import setup, find_packages

setup(
    name='scrapinghub-entrypoint-scrapy',
    version='0.11.0',
    license='BSD',
    description='Scrapy entrypoint for Scrapinghub job runner',
    long_description=open('README.rst').read(),
    packages=find_packages(),
    install_requires=[
        'Scrapy>=1.0',
        'scrapinghub>=1.9.0',
    ],
    entry_points={
        'console_scripts': [
            'start-crawl = sh_scrapy.crawl:main',
            'list-spiders = sh_scrapy.crawl:list_spiders',
            'shub-image-info = sh_scrapy.crawl:shub_image_info',
        ],
    },
    classifiers=[
        'Framework :: Scrapy',
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Topic :: Utilities',
    ],
)
