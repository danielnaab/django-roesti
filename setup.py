#!/usr/bin/env python
from distutils.core import Command
from setuptools import setup, find_packages

from roesti import __version__


class TestCommand(Command):
    user_options = []

    def initialize_options(self):
        pass

    def finalize_options(self):
        pass

    def run(self):
        from django.conf import settings
        settings.configure(
            DATABASES={
                'default': {
                    'NAME': ':memory:',
                    'ENGINE': 'django.db.backends.sqlite3'
                }
            },
            INSTALLED_APPS=(
                'roesti',
            )
        )

        import django
        if django.VERSION[:2] >= (1, 7):
            django.setup()

        from django.core.management import call_command
        call_command('test', 'roesti')


def readme():
    with open('README.md') as f:
        return f.read()


setup(
    name='django-roesti',
    version=__version__,
    packages=find_packages(exclude=('tests*',)),
    include_package_data=True,
    author='Daniel Naab',
    author_email='dan@crushingpennies.com',
    description='Provides a Django model keyed on the hash of its contents.',
    long_description=readme(),
    url='https://github.com/danielnaab/django-roesti',
    license='BSD',
    keywords='django',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'License :: OSI Approved :: BSD License',
        'Intended Audience :: Developers',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Framework :: Django',
        'Framework :: Django :: 1.9',
        'Framework :: Django :: 1.10'
    ],
    cmdclass={'test': TestCommand},
)
