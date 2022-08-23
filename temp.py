import os
import pyspark
import subprocess
path = os.path.dirname(pyspark.__file__)
print(path)
print(os.path.abspath(os.getcwd()))
file = os.path.abspath(os.getcwd())
command = f'{path}\\bin\\spark-submit "{file}\\runsp.py"'
print(command)
os.system(command)
a=''
op = subprocess.run(command,capture_output=True,shell=True)
print(op.returncode)
print(op.stdout)
print(op.stderr)