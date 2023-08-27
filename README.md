# Data Engineering Demo

## Airflow Installation

Here is a recipe for having Airflow run locally to execute the DAGs
in this repository.

Make sure you have python installed on your machine.
In your shell enter directory which also hosts this README.

If not already in a virtual environment. Create one.

```sh
python3 -m venv .venv
source .venv/bin/activate
```

Then run the following shell commands to install airflow and
prepare for a local Airflow project.

```sh
export AIRFLOW_HOME=$PWD/.cfg
export OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES
export AIRFLOW_VERSION=2.2.2
export PYTHON_VERSION=`python3 -c 'import sys; print(".".join([str(sys.version_info.major), str(sys.version_info.minor)]))'`
echo apache-airflow==$AIRFLOW_VERSION --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt" > requirements.txt
pip install -r requirements.txt
```

Once installed, run the following shell commands to set up the database in airflow providing a password for the **admin** user.

```sh
airflow db init
sed -i '' 's/load_examples = True/load_examples = False/g' '.cfg/airflow.cfg'
sed -i '' 's/.cfg\/dags/dags/g' '.cfg/airflow.cfg'
airflow db reset -y
airflow users create \
    --username admin \
    --firstname Peter \
    --lastname Parker \
    --role Admin \
    --email spiderman@superhero.org
```

Then to run:

```sh
airflow standalone
```

Airflow web should now be running on your computer on localhost port 8080 and the DAGs should automatically schedule in the background.
