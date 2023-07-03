# Set enviornment variables

export PYTHONPATH=${PWD}
export environment="development"

# Run Tests
rm -rf allure_results/history
cd tests && pytest --alluredir=allure-results .
cp -R allure-report/history allure-results/
allure serve allure-results