import datetime as dt
import inspect
import pathlib

def default_settings():
	frame = inspect.stack()[1]
	module = inspect.getmodule(frame[0])
	filename = module.__file__


	settings = {
		'dag_id': pathlib.Path(filename).stem,
		'schedule_interval': '@daily',
		'catchup': False,
		'default_args': {
		'owner': 'Morozov',
		'start_date': dt.datetime(2021, 4, 16),
		'retries': 1,
		'retry_delay': dt.timedelta(minutes=1),
		'depende_on_past': False,
		}
	}

	return settings
