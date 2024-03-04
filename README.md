# PySpark Client Data Processor
This PySpark application processes client data from two separate datasets from **KommatiPara**. The application collates information about specific clients and their financial details for a new marketing push by the company. 

Specifically, this application:

- Removes personal identifiable information and credit card numbers
- Selects clients from the United Kingdom or the Netherlands.
- Joins the filtered datasets based on client identifiers.
- Renames columns for better readability.
- Saves the resulting dataset to a specified directory.
- Accepts command-line arguments for specifying dataset paths and target countries.

## Install
This project is made inside a Visual Studio Code devcontainer for easy shareability without worrying about dependencies. It can also be installed manually.

### Install through Visual Studio Code devcontainer
Make sure to have the 'Dev Containers' extension installed in VSCode and the Docker deamon running. Then clone the project and reopen it in a container.

### Install manually
1. Ensure Python 3.8 and Java are installed on your system.
2. Clone this repository to your local machine.
3. From the root directory, run `pip install -r requirements.txt` to install the remaining packages.

## Usage
When everything is installed, you can run the application with the following command:

 `python main.py <PATH_TO_CLIENT_DATASET> <PATH_TO_FINANCIAL_DATASET> <COUNTRIES>`

 Where:
 - PATH_TO_CLIENT_DATASET corresponds to the path of `dataset_one.csv`
 - PATH_TO_CLIENT_DATASET corresponds to the path of `dataset_two.csv`
 - COUNTRIES corresponds to the countries that you want to filter on, passed as a single argument representing the list of countries separated by a comma.

For example you can use:
`python main.py "/input/dataset_one.csv" "/input/dataset_two.csv" "Netherlands,United Kingdom"`
