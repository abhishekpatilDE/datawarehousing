# datawarehousing
DataWarehousing Concepts

OPEN SCHEMA.PY FOR DATABRICKS EASY CODE AND UNDERSTANDING 

## Run locally

These instructions show how to create a virtual environment, install Python dependencies (including `pyspark`), and run the `incrementload.py` script from this repository.

1. Create a virtual environment (if you don't already have one):

```bash
python3 -m venv .venv
.venv/bin/python -m pip install --upgrade pip
```

2. Install dependencies from `requirements.txt`:

```bash
.venv/bin/python -m pip install -r requirements.txt
```

3. Run the example script with the venv Python:

```bash
.venv/bin/python incrementload.py
```

4. Java requirement

PySpark requires a Java runtime (JRE/JDK). Confirm Java is available in the environment/container:

```bash
java -version
```

If Java is missing, install an OpenJDK (for example OpenJDK 11+) or set `JAVA_HOME` to a valid JDK path.

5. Troubleshooting

- If you see "ModuleNotFoundError: No module named 'pyspark'", ensure you installed `pyspark` into the active venv and that you're running the script with the venv Python (see command above).
- If you connect to an external Spark cluster, make sure the pyspark version matches the cluster's Spark version.

6. Pinning versions

Consider pinning a `pyspark` version in `requirements.txt`, e.g. `pyspark==3.4.1`, to ensure consistent behavior across environments.

If you'd like, I can add a small shell helper (setup script) to automate creating the venv and installing requirements.
