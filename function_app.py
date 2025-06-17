import azure.functions as func
from code_to_import import helper_code
import logging
import os

#app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)
app = func.FunctionApp()

@app.function_name("ExampleFunction")
@app.route(route="examplefunction")
def example_function_http_trigger(req: func.HttpRequest) -> func.HttpResponse:
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    logging.info("Your example function was successfully triggered.")

    # do something with the req argument
    events = req.get_json()
    logger.info(f"Request body:\n{events}")

    # execute imported functions
    helper_code()

    # put your own code here ...


    # finish execution
    logger.info("Finished execution.")
    return func.HttpResponse()

@app.function_name("AnotherExampleFunction")
@app.route(route="anotherexamplefunction")
def another_example_function_http_trigger(req: func.HttpRequest) -> func.HttpResponse:
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    logging.info("Another example function was successfully triggered.")

    # do something with the req argument
    events = req.get_json()
    logger.info(f"Request body:\n{events}")

    # execute imported functions
    helper_code()

    # put your own code here ...

    # finish execution
    logger.info("Finished execution.")
    return func.HttpResponse()


#app = func.FunctionApp()

@app.timer_trigger(schedule="0 0 14,18,22 * * *", arg_name="myTimer", run_on_startup=False,
              use_monitor=False) 
def lendz_sync_salesforce(myTimer: func.TimerRequest) -> None:
    logging.info('====== sync_loanpass function executed.')
    my_connection_string = os.getenv('SQL_CONNECTION_STRING')
    if not my_connection_string:
        raise ValueError("SQL_CONNECTION_STRING environment variable not set. Please set it before running the script.")
    # Call the new orchestrating function to fetch data from LoanPASS API and process it
    helper_code()