from setuptools import setup, find_packages
import os

readme_path = os.path.join(os.path.dirname(__file__), "README.md")
with open(readme_path, "r") as readme_file:
    readme = readme_file.read()

with open('requirements.txt', 'r') as f:
    required = f.read().splitlines()

setup(
    name = 'cell_report_card',
    version = '0.1.0',
    description = "Run custom Patch-seq Cell Report Card query on LIMS.",
    long_description=readme,
    author = "Sarah Walling-Bell",
    author_email = "sarah.wallingbell@alleninstitute.org",
    url = 'https://github.com/sarahwallingbell/cell_report_card',
    packages = find_packages(),
    install_requires = required,
    include_package_data=True,
    package_data={"cell_report_card": ["data/*"]},
    setup_requires=['pytest-runner'],
)