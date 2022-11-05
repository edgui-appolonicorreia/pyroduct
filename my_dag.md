# Airflow ignore file
By using the .airflowignore you can put all files and directories that you don't want the Airflow scheduler parse.

# Creating a custom operator from the PostgresOperator
class CustomPostgresOperator(PostgresOperator):

    template_fields = ("sql", "parameters")

    def _extract(partner_name):
        print(partner_name)

## This way, avoid you set 'dag=dag' in each task
with DAG(
        "my_dag",
        description="This is a test DAG.",
        start_data=datetime(2021, 1, 1),
        schedule_interval=None,
        dagrun_timeout=timedelta(minutes=5),
        tags=["Testing", "Label_A"],
        catchup=False
        ) as dag:
    DummyOperator()

EACH TASK CAN HAVE YOUR OWN START DATE
The DAG will triggered when arrive the start_date + schedule interval.
The CRON expression is 'stateless' and the timedelta is 'statefull'.
If the DAG has start_date=datetime(2021, 1, 1, 10, 50) and a schedule_interval="@daily" or schedule_interval="0 0 * * *", so this DAG will triggered again at the 00:00 at the next day.
If the DAG has start_date=datetime(2021, 1, 1, 10, 50) and a schedule_interval=timedelta(days=1), so the DAG will triggered again, 24 hours later, or: at 10:50AM of the next day.

Idempotent: A Task that is 'idempotent' it's a task that ALWAWYS returns or generate the same effect or result.
If your task can't run more than 1 time, your task isn't idempotent and determinisc. For example:

	PostgresOperator(task_id="create_table", sql="CREATE TABLE my_table ...")

If I try to execute this task with this operator, I'm able to execute this task ONLY ONE UNIQUE TIME. But if I write the SQL command like this:

	PostgresOperator(task_id="create_table", sql="CREATE TABLE IF NOT EXIST ...")

## Using Jinja template with variables
A good pratice it's to fetch the variables using the template Jinja... For example: I have a variable called 'my_dag_partner' with a json value {"name": "partner"}.
If I want to have the name value of this variable I can do like this:
    extract = PythonOperator(
        task_id="extract",
        python_callable= some_python_func
        op_args=["{{ var.json.my_dag_partner.name }}"]
    )

## Environments Airflow vars
By using the sintaxe 'AIRFLOW_ENV_' you can create a env variable. This kind of variable, you can't see at the UI, but you can call into the tasks using the Jinja template.

# Backfilling: You can run the DAG from the start date until now. Just setting the 'catchup' as True.
# You also can run from CLI (console) typping the command 'airflow dags backfill -s 2020-01-01 -e 2021-01-01'.
# It's possible to run the DAG, that already had run previously by the UI.

# Using Jinja templates in the DAGs.
    
    extract = PostgresOperator(
            task_id="fetching_data",
            sql="SELECT partner_name FROM partners WHERE date={{ ds }};"
            )

IMPORTANT: The "{{ ds }}" of template Jinja, correspond at the current execution datetime of the DAG run!

## The rendered query, previously wrote, would be 'SELECT partner_name FROM partners WHERE date=2021-08-31', the date of today. The "{{ ds }}" means the execution date of the DAG.
## You also can use template Jinja engine in .sql files.

## To use the custom operator that I created before...

class CustomPostgresOperator(PostgresOperator)

    template_fields = ("sql", "parameters")

    task2 = CustomPostgresOperator(
            task_id="custom_operator",
            sql="sql/MY_REQUEST.sql",
            parameters={"next_ds": "{{ next_ds }}", "prev_ds": "{{ prev_ds }}", "partner_name": "{{ var.json.my_dag_partner.name }}"}
            )

# To use XCOM, one option is to use de 'ti' (task instance) at the function: 'def extract(ti):' and using ti.xcom_pull and ti.xcom_push .
# Another way to use xcom, just a function returning the value and the following function getting the returned value using the 'task_ids' parameter:

def _extract(ti):
    partner_name = "netflix"
    partner_path = "/partners/netflix"
    return {"partner_name": partner_name, "partner_path": partner_path}

def _process(ti):

    partner_settings = ti.xcom_pull(task_ids="extract")
    print(partner_settings["partner_name"])

# Task Flow API:
    # 1 - Decorators: 
        # - @task.python: using this decorator at the top of the python function that I want that execute, automatically airflow will create the PythonOperator for me in my DAG;
        # - @task.virtualenv: execute the python function using a specfic virtual env;
        # - @task_group: allow create and group tasks;
    # 2 - Xcom Args: it's a way to make XCOM explicits from tasks' dependecies.

## Using decorators

from airflow.decorators import task, dag

@task.python
def extract():
    partner_name = "netflix"
    return partner_name

@task.python
def process(partner_process):
    print(partner_process)

@dag(description="testing operators", schedule_interval=None)
def my_dag():
    
    process(extract())

my_dag()

## ... or:

def my_dag():
	
	extract() >> process()

# If I want to share multiple values as XCOM?

@task.python(task_id="extract_partners", multiple_outputs=True)
def extract():
	partner_name = "netflix"
	partner_path = "/partners/netflix"
	return {"partner_name": partner_name, "partner_path": partner_path}

# Whats is happening here? The parameter 'multiple_outputs' as True will understand the function's dict return as multiple XCOMs and send each one as diferents XCOMs. So, if the returns dict has 2 keys, will generated 2 XCOMs. If the returns dict has 4 keys, will generate 4 XCOMs.

# Another equivalent 'multiple_outputs':
@task.python(task_id="extract_partners")
def extract() -> Dict[str, str]:
	patner_name = "netflix"
	partner_path = "/partners/netflix"
	return {"partner_name": partner_name, "partner_path": partner_path}

# Getting multiples XCOMs

from airflow.decorators import task, dag

@task.python(task_id="extract_partners", multiple_outputs=True)
def extract():
	partner_name = "netflix"
	partner_path = "/partners/netflix"
	return {"partner_name": partner_name, "partner_path": partner_path}

@task.python
def process(partner_name, partner_path):
	print(partner_name)
	print(partner_path)

@dag(description="testing operators", schedule_interval=None)
def my_dag():

	partner_settings = extract()
	process(partner_settings["partner_name"], partner_settings["partner_path"])

dag = my_dag()

# Another important info: the '@task.python' parameters 'do_xcom_push' as False, avoid to send an entire return's value of the function to XCOM as string.

## VARIABLES

# When you use "_secret" at the end of the variable's title, the variable becomes a secret into the UI Airflow, showing like this "******".

# Every time that I use "Variable.get", the Airflow opens a connection with the DB and try to get the var that I wrote. If I use a template JINJA like "{{ var.json.my_var_here.some_key }}", the Airflow will open a connection and try to access the var only when the task will be started.

A good pratice it's try to fetch the variables inside each task or using Jinja template!

# Into the Airflow dockerfile, if you type 'ENV AIRFLOW_VAR_{NAME}' the Airflow will automatically understand this as a environment Airflow variable. This kind of var,
# doesn't appear at the Airflow variables page, it's hidden and only one who has access to the machine, can see and edit this variable.

The 6 ways to create a variable in Airflow:
- Airflow UI
- Airflow CLI
- REST API
- Environment Variables️
- Secret Backend️
- Programatically 

# SubDAGs
Bad idea to use it, takes too much work. Better to use Tasks Groups!

# Task Groups and dynamic tasks
The base operator > from airflow.utils.task_group import TaskGroup
The decorator > from airflow.decorators import task, task_group

## Dynamic tasks
This it's possible, 'cuz the tasks will bre created based into dict. So, let's define the dict:
partners = {
    "partner_snowflake": {"name": "snowflake", "path": "/path/snowflake"},
    "partner_netflex": {"name": "netflix", "path": "/path/netflix"},
    "partner_astronomer": {"name": "astronomer", "path": "/path/astronomer"}
}


default_args={"start_date": datetime(2021, 1, 1)}

@dag(dag_id="your-dag-id-here", description="some description here", default_args=default_args, schedule_interval="@daily", ...)
def my_dag():

	start = DummyOperator(task_id="start")

	for partner, details in partner.items():

		@task.python(task_id=f"extract_{partner}", do_xcom_push=False, multiple_outputs=True)
		def extract(partner_name, partner_path):
			return {"partner_name": partner_name, "partner_path": partner_path}
		extracted_values = extract(details["name"], details["path"])
		start >> extracted_values

# Branching

Nothing new here!

# Dependencies

[t1, t2, t3] >> [t4, t5] THIS WAY IT'S TOTALLY WRONG!

You can use cross_downstream operator to set it! from airflow.models.baseoperator import cross_downstream

Example: cross_downstream([t1, t2, t3], [t4, t5])

And if you need to use tasks later cross_downstream, you CAN'T use cross_downstream([t1, t2, t3], [t4, t5]) >> t6
The correct way it's: [t4, t5] >> t6

## Chain dependencies
It's a complex and specific way to build dependencies
Example: chain(t1, [t2, t3], [t4, t5], t6)

# Parallelims and concurrency
PARALLELISM = the number of tasks that can be executed at the same time in the entire Airflow instance;
DAG_CONCURRENCY = the number of tasks of a given DAG that can be executed at the same time into a DAG run. The default value is 16;
MAX_ACTIVE_RUNS_PER_DAG = the max value of how many times a DAG can run at the same time. The default value is 16;

## Pool
Pools are kind of very customizable ways to set DAG_CONCURRENCY.
Are very usefull when you have to run at the same time process that can use a lot of memory and computing power, and can save a lot of money when you use a cloud.
The slots means how much tasks you wants running at the same time. Also it's possible set manually for any reason the number of slots using the parameter pool_slots,
at the @task.python .
SubDAG doens't respects pools.

# Task priority
You can set the priority of tasks using the parameter priority_weight into the @task.python .
The priority parameter works only if the tasks are all into the same pool!

# Depend on past
The depends_on_past it's not about tasks, but about DAG RUNS!
So if a task 'A' with depends_on_past as true fails, at the second DAG run, the task A will be not triggered.
You can trigger manually the DAG and the parameter will be ignored.

# Wait of downstream
This parameters is kind of automatic depends on past as true. When you have a DAG runs 2 times at the same time, and the task 'A' it's successully executed,
the task 'A' of the second DAG RUN will be also executed.

# Sensors
Sensors are an operators waiting some condition to be true. Sensors uses workers!

Example:
from airflow.sensors.date_time import DateTimeSensor

@dag(description="Some desc here", ...)
def my_dag():
    
    start = Dummy Operator(task_id="start", trigger_rule="dummy")

    delay = DateTimeSensor(
        task_id="delay",
        target_time="{{ execution_date.add(hours=9) }}",
        poke_interval=60, # This parameter checks the frequency if your condition it's true
        mode="poke" # It's how the sensor works, this way consumes a worker and a slot into the pool and this mode doesn't set free the worker
        mode="reschedule" # This way of mode, chekcs if the condition it's true, if not then the DAG set free the worker and in 1 hour checks again.
        timeout=60*60*10 # Default value: seven days. If the condition is never true, so after seven days, this sensor will expirate. It's a deadline for the sensor.
        execution_timeout="" # 
        soft_fail=True # As soon the sensor timeout expires, then the sensor won't fail but will be skipped. As true, this parameter just will "said" that the Sensor won't failed, but was skipped.
        exponencial_backoff=True # Will increase the time between each poke.
)

# Timeout
The DAG run timeout parameter (`dagrun_timeout`) will not be considered if you trigger manually the DAG.
The timeout for tasks it's `execution_timeout`. It's a good pratice you define both timeouts, for DAG and task.

# Failures and retries

## Callbacks for DAG
You can set a DAG parameter called `on_success_callback`, to call a python func to send a message to your slack or record a data into a DB with the success data.
But, if your DAG fails, you can use the parameter `on_failure_callback` and to the same. 
IMPORTANT: if you have a FAIL inside the python callback func, there is no chance to retry.

## Callbacks on Tasks level
ON SUCCESS CALLBACK: `on_success_callback`
ON FAILURE CALLBACK: `on_failure_callback`
ON RETRY CALLBACK: `on_retry_callback`

If you want to know why you task fails, some time, the Airflow will record the error inside the key `exception` and you can look for this key inside the `context`. If there is this key, so your task fails at some point and you can print or store this error message.
Example to figure out an error about timeout exception:

from airflow.exceptions import AirflowTaskTimeout, AirflowSensorTimeout
def _callback_failure(context):
    if (context["exception"]):
        if (isinstance(context["exception"], AirflowTaskTimeout)):
            print("Task Timeout exception!")
        elif (isinstance(context["exception"], AirflowSensorTimeout)):
            print("Sensor timeout exception")
        else:
            print("Another problem!")
    else:
        print("FAIULRE CALLBACK")

### Retry func error print example
from
def _callback_retry(context):
    if (context["ti"].try_number() > 2):
        print("The task retries 2 or more times")
    else:
        print("bla bla")

# Different ways to retry your tasks
The parameter `retries` at DAG level it's overwritten by the same parameter at the task level.
The parameter `retry_dalay` means the time interval to wait between the retries. This parameter waits a timedelta object.
The parameter `retry_exponential_backoff` it's boolean and increse exponentially the time interval between the retries.
The parameter `max_retry_delay` it's the max time that you want to wait as interval between your retries.

# SLAs
The diference between timeouts and SLAs is: timeouts STOPS the DAG or TASK, SLA NOT! SLA verify if your DAG it's complete in given period of time.

You can use it setting the task parameter `sla` and expects a timedelta object. The SLA can be defined into the task, but this parameters it's for ALL DAG and not for a specific task. So, if you define a 5 minutes SLA into the first task of a DAG, the SLA of that given DAG is 5 minutes and not only for the task. Given that, you can define the SLA parameter only in unique task, the first one or the last one.

It's possible call function when a DAG miss the SLA using the parameter, at DAG level, `sla_miss_callback`. Example:

def _sla_miss_callback(dag, task_list, blocking_task_list, slas, blocking_tis):
    # `dag`: general info about dag;
    # `task_list`: the task instances objects
    # `blocking_task_list`: list of tasks that missed the SLA 'cuz another task missed;
    # `slas`: list of tasks with SLA;
    # `blocking_tis`: list of tasks that miss the SLA, tasks that tooks so much time and miss the SLA;

If the DAG it's triggerd MANUALLY the SLA is not checked!

#####################################
Dynamic DAGs
What a such important topic! 🤩

The concept of dynamic if fairly simple.  Whenever you have multiple DAGs that  have the same tasks for which only the inputs change, then  it might be better to generate those DAGs dynamically. 

The idea is  to save  a ton of time, writing the same DAGs again and again just for slight differences.

Now, there are two ways of generating DAGs dynamically. Let's begin by the first one.

The Single-File Method
I don't really like this one and if you use it, well, read what comes next, it's gonna be very useful for you.

The Single-File Method  is based on a single Python file in charge of generating all the DAGs based on some inputs.

For  example, a list of customers, a list of tables, APIs and so on.

Here is an example of implementing that method:

from airflow import DAG
from airflow.decorators import task
from datetime import datetime

partners = {
    'snowflake': {
        'schedule': '@daily',
        'path': '/data/snowflake'
    },
    'netflix': {
        'schedule': '@weekly',
        'path': '/data/netflix'
    }, 
}

def generate_dag(dag_id, schedule_interval, details, default_args):

    with DAG(dag_id, schedule_interval=schedule_interval, default_args=default_args) as dag:
        @task.python
        def process(path):
            print(f'Processing: {path}')
            
        process(details['path'])

    return dag

for partner, details in partners.items():
    dag_id = f'dag_{partner}'
    default_args = {
        'start_date': datetime(2021, 1, 1)
    }
    globals()[dag_id] = generate_dag(dag_id, details['schedule'], details, default_args)
Here, you are going to generate  2 different DAGs according to your partners. You will end up with:

(image from Airflow UI with 2 dags named: "dag_netflix" and "dag_snowflake")

All good! But, there are some drawbacks with this method.

DAGs are generated every time the Scheduler parses the DAG folder (If you have a lot of DAGs to generate, you  may experience performance issues
There is no way to see the code of the generated DAG on the UI
If you change the number of generated DAGs, you will end up with DAGs that actually don't exist anymore on the UI
So, yes it's easy to implement, but hard to maintain. That's why, I usually don't recommend that method in production for large Airflow deployements.

That being said, let's see the second method, which I prefer a lot more! 🤩

The Multi-File Method
This time, instead of having one single Python file in charge of generating your DAGs , you are going to use a script that will  create a file of each generated DAG. At the end you will get one Python File per generated DAG.

I won't give the full code here,  but let me give you the different steps:

Create a template file corresponding to your DAG structure with the different tasks. In it, for the inputs, you put some placeholders that the script will use to replace with the actual values.
Create a python script in charge of generating the DAGs by creating a file and replacing the placeholders in your template file with the actual values.
Put the script, somewhere else than in the folder DAGs.
Trigger this script either manually or with your CI/CD pipeline.
The pros of this method are multiples!

It is Scalable as DAGs are not generated each time the folder dags/ is parsed by the Scheduler
Full visibility of the DAG code (one DAG -> one file)
Full control over the way DAGs are generated (script, less prone to errors or "zombie" DAGs)
Obviously, it takes more work to set up but if you have a lot of DAGs to generate, that's truly the way to go!

########################################

# DAG dependencies with ExternalTaskSensor
You can use this sensor to check the state of some task in another DAG. This sensor will check a given task status into a specific execution datetime.
Example:

waiiting_for_task = ExternalTaskSensor(
    task_id="waiting_for_task",
    external_dag_id="my_dag",
    external_task_id="storing"
    execution_delta=timedelta() # This parameters you set when you don't want the same execution datetime
    failed_states=["failed", "skipped"] # A list of issues that you fail the actual DAG and prevent for don't run forever
    allowed_states=["success"] # A list of states that allows the actual DAG runs
)

# TriggerDagRunOperator
You can trigger a DAG from another DAG.
Example:

trigger_other_dag=TriggerDagRunOperator(
    task_id="trigger_other_dag",
    trigger_dag_id="other_DAG" # The other DAG that you want to trigger
    execution_date="{{ ds }}" # The execution datetime that you want to run the other DAG, you can use a jinja template, a string or a datetime object.
    wait_for_completion=True # If you want to wait complete/finish the triggered DAG to move to the next task
    poke_interval=60 # Time interval that will check if the triggered DAG it's commpleted or not
    reset_dag_run=True # Reset the triggered DAG 'cuz you can't run twice the same DAG at the same execution datetime
    failed_state=["failed"] # Specifies the states that you don't want to trigger the target DAG
)

Cupom code for retry:
free-retry-exam-airflow-dag-authoring
