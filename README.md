# __Compile Ticker Data__



## __Project Purpose__

To compile ticker data from internet resources to explore and analyse the ETF and equity trades. 



## __Main Tools__

Tools | Description
:--- | :---
[luigi][luigi_docs_url] | For building data pipeline and dashboard. 
[pandas][pandas_docs_url] | For data processing. 
[pipenv][pipenv_docs_url] | For managing dependencies. 



## __Data Architecture Overview__

This project focuses on the __Streamlit Dashboard__ section. It's associated with [Market Trades Dashboard Repo][market_trades_dashboard_repo] 
(Streamlit Dashboard section). 

![Project Architecture Diagram][architecture_overview_img] 



## __Folder Structure__

File / Folder Name | Description
:--- | :---
autovisualise_data | For keeping custom python modules related to data visualisation. 
config | For configuration. It encompasses 3 files. `config_logger` is for logger, `config_dashboard` for dashboard, `config_naming` for namings, `config` for other general configuration. 
docs | For storing files, data, and documents. 
logs | For storing the log info. 
sh | For running bash script on Mac. 
pipfile | For setting up the virtual environment and tracking all the installed dependencies. 



## __Project Setup__

1.  Pull the project from the repo. 

1.  Install `pyenv` to manage the python version (if needed) and `pipenv` for dependencies. 

1.  Run this to install dependencies from the `Pipfile.lock` file. 
    
    ```bash
    pipenv shell;
    pipenv sync; 
    ```

1.  To reinstall the entire dependencies, update the version within the `Pipfile` file if needed, then run this. This will 
    automatically create / update the `Pipfile.lock` file for you. 

    ```bash
    pipenv install;
    ```

1.  Try import specific library and check the version to see if it's installed. 



## __Code Running Guide__

1.  Run the streamlit dashboard app locally. 

    ```bash
    streamlit run run_streamlit.py
    ```

1.  Run the docker compose to start the app locally. 

    ```bash
    sh sh/run_docker_compose.sh up
    ```

    To stop the app from running, do this. 

    ```bash
    sh sh/run_docker_compose.sh down 
    ```



## __Deployment Guide__

Updating soon... 



## __Debugging & Testing__ 

1.  To debug specific functions or code, use [Jupytext Percent Format][jupytext_percent_docs_url]. 
    Simple include the following at the bottom of the `run_pipeline.py` file and write your code 
    to test specific function(s). This relies on `ipykernel` module so ensure that it is installed 
    via `pipenv`. 

    ![Jupytext Percent Example][jupytext_percent_img]



[architecture_overview_img]: ./docs/images/architecture_overview.jpg 
[config_debug_img]: ./docs/images/config_debug_example.png
[jupytext_percent_img]: ./docs/images/jupytext_percent_example.png

[market_trades_dashboard_repo]: https://github.com/lioneltayyd/market-trades-dashboard

[luigi_docs_url]: https://luigi.readthedocs.io/en/stable/
[pandas_docs_url]: https://pandas.pydata.org/docs/user_guide/index.html
[pipenv_docs_url]: https://pipenv-fork.readthedocs.io/en/latest/
[jupytext_percent_docs_url]: https://jupytext.readthedocs.io/en/latest/formats.html#the-percent-format
