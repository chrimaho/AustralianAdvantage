import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description=fh.read()

setuptools.setup(
    name="AdvantageVisualisation-chrimaho", # Replace with your own username
    version="0.0.1",
    author="Chris Mahoney",
    author_email="chrismahoney@hotmail.com",
    description="A visual analysis of the different levels of advantage present within Australia.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/chrimaho/AustralianAdvantage",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.7',
)