ETL Pipeline
==============================

A project to develop ETL jobs for public data sets as a Github Action, for publication as Github Pages.

Project Organization
------------

    ├── README.md          <- The top-level README for developers using this project.
    ├── .gitignore         <- NEVER DELETE THIS FILE.
    ├── data
    │   ├── downloaded     <- Data from downloads.
    │   ├── interim        <- Intermediate data that has been transformed.
    │   ├── processed      <- The final, canonical data sets for publication.
    │   └── raw            <- Original, immutable data exports.
    │
    ├── requirements.txt   <- The requirements file for reproducing the environment, e.g.
    │                         generated with `pip freeze > requirements.txt
    ├── .env               <- Environment variables                          
    │
    ├── main.py            <- Launcher to run all ETL jobs
    │                          
    └── jobs/*.py          <- ETL scripts for individual data sets
    

--------

<p><small>Project based on the <a target="_blank" href="https://drivendata.github.io/cookiecutter-data-science/">cookiecutter data science project template</a>. #cookiecutterdatascience</small></p>
	
