import logging
import azure.functions as func
import ingest_data
from datetime import datetime, timezone

app = func.FunctionApp()

@app.timer_trigger(schedule="0 */5 2-16 * * *", arg_name="myTimer", run_on_startup=True,
              use_monitor=True) 
def raw_api_ingest_cidc_v1(myTimer: func.TimerRequest) -> None:
    if myTimer.past_due:
        logging.info('The timer is past due!')

    logging.info("Ingest timer triggered at %s", datetime.now(timezone.utc).isoformat())
    try:
        ingest_data.main()   # call your existing script's main()
        logging.info("Ingest script completed successfully.")
    except Exception as e:
        logging.exception("Ingest script failed: %s", e)
        # re-raise so Functions records failure
        raise