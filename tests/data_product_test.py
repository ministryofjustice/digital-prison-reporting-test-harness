import re,sys,os
sys.path.append(os.pardir)
from playwright.sync_api import Page, expect
from utils.Config import getConfig



data_product_url= getConfig("data_product", "login_page")

def test_login_page(page: Page):
    page.goto(data_product_url)

    # Expect page Title to be ""HMPPS Digital Services - Sign in""
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
    
def test_login_failure(page:Page):
    
    # Password field
     
    username_field= page.query_selector('input[name="username"]')
    
    # Password field
    password_field=page.query_selector('input[name="password"]')
    
    # Submit button
    submit_button=page.query_selector('#submit')
    
    # Login with incorrect credentials
        
    username_field.fill("unknown_user")
    password_field.fill("incorrect_password")
    submit_button.click()
    