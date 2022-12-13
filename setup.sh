#!/bin/sh

# Check for update system update and install them
echo '###########'
echo 'Check for update system update and install them'
sudo apt-get update && sudo apt-get upgrade -y
# Install wget and dpkg
echo 'Install wget dpkg'
sudo apt-get install wget unzip -y
# Get google chrome browser 
echo 'Get google chrome browser .deb package'
wget https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb
#Install the downloaded Chrome package
sudo apt --fix-broken install  google-chrome-stable_current_amd64.deb -y

# Get chromedriver
wget https://chromedriver.storage.googleapis.com/2.41/chromedriver_linux64.zip
# Unzip chromedriver
unzip chromedriver_linux64.zip
#  move chromedriver
sudo mv chromedriver /usr/bin/chromedriver

# sudo apt-get install firefox
# wget https://github.com/mozilla/geckodriver/releases/download/v0.32.0/geckodriver-v0.32.0-linux32.tar.gz
# tar -xvzf geckodriver*
# chmod +x geckodriver
# sudo mv geckodriver /usr/bin/.
# install postgresql
#sudo apt-get install postgresql postgresql-contrib -y
# Install pip3
sudo apt-get install python3-pip -y

# upgrade pip
pip install --upgrade pip

# install requirements
pip install -r requirements.txt
