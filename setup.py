from setuptools import setup, find_packages

setup(
    name='scrapinghub-entrypoint-scrapy',
    version='0.14.0',
    license='BSD',
    description='Scrapy entrypoint for Scrapinghub job runner',
    long_description=open('README.md').read(),
    packages=find_packages(),
    install_requires=[
        'Scrapy>=1.6',
        'scrapinghub>=2.1.0',
    ],
    entry_points={
        'console_scripts': [
            'start-crawl = sh_scrapy.crawl:main',
            'list-spiders = sh_scrapy.crawl:list_spiders',
            'shub-image-info = sh_scrapy.crawl:shub_image_info',
        ],
    },
    python_requires='>=3.6',
    classifiers=[
        'Framework :: Scrapy',
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Topic :: Utilities',
    ],
)
