from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.keys import Keys
from selenium import webdriver
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
import pandas as pd
import getpass
import time
import pickle


username = input(username)
password = input(password)

bot = webdriver.Chrome()
bot.implicitly_wait(0.5)
url = 'https://www.capitaliqpro.com'
bot.get(url)
time.sleep(10)
#bot.find_element("name", "username").send_keys(username)
bot.find_element(By.ID, "input28").send_keys(username)
print('username entered')
time.sleep(5)
bot.find_element(By.CSS_SELECTOR, "input.button.button-primary").click()
time.sleep(5)
bot.find_element(By.ID, "input60").send_keys(password)
print('password entered')
time.sleep(5)
bot.find_element(By.CSS_SELECTOR, "input.button.button-primary").click()
#pwd = bot.find_element('name','password')
#name="password"
#pwd.send_keys(password)
#pwd.send_keys(Keys.RETURN)
time.sleep(15)
cookies=bot.get_cookies()
pickle.dump(cookies,open('./cookies.pkl','wb'))

#<input type="text" maxlength="200" placeholder="Search e.g. &quot;SPGI FIN SUPP&quot;" tabindex="0" autocapitalize="none" autocomplete="off" autocorrect="off" spellcheck="false" aria-autocomplete="list" class="css-16oq7a8" value="">
