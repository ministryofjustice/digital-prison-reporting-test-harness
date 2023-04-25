rm -rf allure_results/history
cd tests && python3 -m pytest --alluredir=allure-results load_job_test.py
# rm -rf allure_results/history
cp -R allure-report/history allure-results/
# allure generate --clean allure-results && allure open
allure serve allure-results