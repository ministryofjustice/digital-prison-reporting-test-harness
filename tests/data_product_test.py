import pytest
import re,sys,os
sys.path.append(os.pardir)
from playwright.sync_api import expect
from faker import Faker

from utils.Config import getBoolean, getConfig

faker = Faker(locale="en_GB")
data_product_url= getConfig("data_product", "login_page")
dps_auth=getConfig("data_product","dev_auth")
dps_username=os.environ.get("dps_username")
dps_password=os.environ.get("dps_password")
random_user_name=faker.name()
random_password=faker.password()

# Set to True of False in test config
headless_mode= getBoolean("data_product","headless_mode")


print("PASSWORD",random_password)
@pytest.fixture(scope="function")
def browser(playwright):
    browser = playwright.chromium.launch(headless=headless_mode)
    yield browser
    browser.close()


def test_login_page(browser):

    page = browser.new_page()
        
    # Perform actions on the page
    page.goto(data_product_url)
    expect(page).to_have_title(re.compile("HMPPS Digital Services - Sign in"))
    
    # Username field 
    username_field= page.query_selector('input[name="username"]')
    # Password field
    password_field=page.query_selector('input[name="password"]')
    # Submit button
    submit_button=page.query_selector('#submit')
    
    # Login page- Username input field exists
    assert username_field is not None
    
    # Login page- Password input field exists
    assert password_field is not None
    
    # Login page- Submit button exists
    assert submit_button is not None

@pytest.mark.skip(reason="Temp failure")
def test_login_failure(browser):

    page = browser.new_page()
  
    page.goto(data_product_url)
    
    # Username field
     
    username_field= page.query_selector('input[name="username"]')
    
    # Password field
    password_field=page.query_selector('input[name="password"]')
    
    # Submit button
    submit_button=page.query_selector('#submit')
    
    # Login with incorrect credentials
        
    username_field.fill(random_user_name)
    password_field.fill(random_password)
    submit_button.click()
    
    current_url= page.url
    expected_url=dps_auth + "auth/sign-in?error=invalid"
                   
    assert current_url == expected_url
    
def test_login_success(browser):

    page = browser.new_page()
  
    page.goto(data_product_url)
    
    # Password field
     
    username_field= page.query_selector('input[name="username"]')
    
    # Password field
    password_field=page.query_selector('input[name="password"]')
    
    # Submit button
    submit_button=page.query_selector('#submit')
    
    # Login with incorrect credentials
        
    username_field.fill(dps_username)
    password_field.fill(dps_password)
    submit_button.click()
    
    expect(page).to_have_title("Digital Prison Reporting MI UI - Home")
    
def test_report_page(browser):

    page = browser.new_page()
  
    page.goto(data_product_url)
    
    # Password field
     
    username_field= page.query_selector('input[name="username"]')
    
    # Password field
    password_field=page.query_selector('input[name="password"]')
    
    # Submit button
    submit_button=page.query_selector('#submit')
    
    # Login with incorrect credentials
        
    username_field.fill(dps_username)
    password_field.fill(dps_password)
    submit_button.click()
    
    expect(page).to_have_title("Digital Prison Reporting MI UI - Home")    
    
    page.goto(data_product_url+"reports/external-movements")
    
    page.wait_for_timeout(1000)
    
    
    # Report UI validations
    
    prison_column_header = page.query_selector('a[data-column="date"]')
    time_column_header = page.query_selector('a[data-column="time"]')
    from_column_header = page.query_selector('a[data-column="from"]')
    to_column_header = page.query_selector('a[data-column="to"]')
    direction_column_header = page.query_selector('a[data-column="direction"]')
    pagination_link= page.query_selector('a.govuk-link.govuk-pagination__link[aria-label="Page 1"]')
    next_page_link= page.query_selector('a.govuk-link.govuk-pagination__link[rel="next"]')
    page_size_select=element = page.query_selector('select.govuk-select')

   
  
    assert prison_column_header is not None
    assert time_column_header is not None
    assert from_column_header is not None
    assert to_column_header is not None
    assert direction_column_header is not None
    assert pagination_link is not None
    assert next_page_link is not None
    assert page_size_select is not None
    


    
    
    
    
    
    