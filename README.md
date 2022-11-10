# Data Validation Tool Using Spark
This is a small utility which leverages pyspark to compare data between two tables existing in two different databases.

## SETUP on linux (cent OS)
To setup on linux run the make file with following commands

`make setup`

`make initialize`

`make build`

Now to run the ui

`make run`

port 5000 is exposed so head to 5000 on ur browser.

 and to kill the container
 
 `make kill`
 
 
 ## Setup on windows
 To launch on windows download java1.8 and python3.8
 then pip all the the requirements under config folder
 
 `pip install -r config/requirements.txt`
 
 To launch the app just trigger the bat file `run.bat`
 
 ## Preview of UI 
 <img width="808" alt="image" src="https://user-images.githubusercontent.com/43238222/186845764-a5863846-0f5d-4653-a3b3-5d172cf65479.png">

![image](https://user-images.githubusercontent.com/43238222/186845939-e1dac64a-fe69-4ad5-8a8a-fec4f0d523f4.png)
