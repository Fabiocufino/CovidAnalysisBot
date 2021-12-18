import typing
import pandas as pd
import arviz as az
import numpy as np

from . data_it import (
    get_and_process_covid_data_it,
    get_raw_covid_data_it,
    process_covid_data_it,
)

from . data_us import (
    get_and_process_covid_data_us,
    get_raw_covid_data_us,
    process_covid_data_us,
)

# Data loading functions for different countries may be registered here.
# For US, the data loader is pre-registered. Additional countries may be
# registered upon import of third-party modules.
# Data cleaning must be done by the data loader function!
LOADERS:typing.Dict[str, typing.Callable[[pd.Timestamp], pd.DataFrame]] = {
    'it': get_and_process_covid_data_it,
    'us': get_and_process_covid_data_us
}


def get_data(country: str, run_date: pd.Timestamp) -> pd.DataFrame:
    """ Retrieves data for a country using the registered data loader method.

    Parameters
    ----------
    country : str
        short code of the country (key in LOADERS dict)
    run_date : pd.Timestamp
        date when the analysis is performed

    Returns
    -------
    model_input : pd.DataFrame
        Data as returned by data loader function.
        Ideally "as it was on `run_date`", meaning that information such as corrections
        that became available after `run_date` should not be taken into account.
        This is important to realistically back-test how the model would have performed at `run_date`.
    """
    if not country in LOADERS:
        raise KeyError(f"No data loader for '{country}' is registered.")
    result = LOADERS[country](run_date)
    assert isinstance(result, pd.DataFrame)
    assert result.index.names == ("region", "date")
    assert "positive" in result.columns
    assert "total" in result.columns
    return result
