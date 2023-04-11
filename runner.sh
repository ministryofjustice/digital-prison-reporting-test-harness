rm -rf allure_results/history
cd tests && pytest --alluredir=allure-results pipeline_test.py
# rm -rf allure_results/history
cp -R allure-report/history allure-results/
# allure generate --clean allure-results && allure open
allure serve allure-results