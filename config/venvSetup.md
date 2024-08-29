As of April 1, 2024, Databricks is using Python 3.8.10. To ensure compatibility, create a virtual environment using the same Python version and test the wheel file within this environment.

# Apple Silicon Mac Setup
### We use x86 emulation with the mac builtin rosetta to ensure packages build correctly.

1. Download a version of conda (miniconda is the smallest) https://docs.anaconda.com/free/miniconda/ \
check install worked with ```conda --version```

2. Create x64 environment with the correct version of python \
    ```CONDA_SUBDIR=osx-64 conda create -n fiscam python=3.8.10```

3. Activate environment \
    ```conda activate fiscam```

4. Add defaults x64 channel \
    ```conda config --prepend channels defaults/osx-64```

5. add conda-forge x64 channel \
    ```conda config --prepend channels conda-forge/osx-64```


7. Run 
<br><br>
# Windows Setup

```bash
python -m venv fiscam
source fiscam/Scripts/activate.bat
pip install --upgrade pip
```
## May have to install microsoft C++ build tools
```bash
https://visualstudio.microsoft.com/visual-cpp-build-tools/
```

---
# Install req.txt
```python -m pip install -r ..\config\requirements.txt``` <br>


## Run Root Scripts
python script with venv <br>
or notebook with jup set up
