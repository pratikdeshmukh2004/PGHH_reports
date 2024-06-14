from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys

import time, json, os
import concurrent.futures

username_value = "abhi"
password_value = "spz@pghh"


def login():
    service = Service()
    options = webdriver.ChromeOptions()
    driver = webdriver.Chrome(service=service, options=options)
    link = "https://www.powerz.in/ipowerz/?db=pithampur&cid=dgfot#signup"
    driver.get(link)
    driver.maximize_window()
    username = driver.find_element(By.NAME, "frmusername")
    username.send_keys(username_value)
    password = driver.find_element(By.NAME, "frmpassword")
    password.send_keys(password_value)
    password.send_keys(Keys.RETURN)
    time.sleep(5)

login()