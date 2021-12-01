Python version -- 3.9.5
Hadoop version -- 3.3.1 
Hive version -- 3.1.2

To execute hive using python install pyhive and some additioonal supporting libraries to 

Connecting HiveServer2 using Python Pyhive

Install Thrift

pip install thrift

Install SASL

pip install sasl

In case if you get an error while installing sasl, follow Pyhive sasl error section below to install dependencies.

Install thrift sasl

pip install thrift_sasl

Installing Pyhive

Once all above packages are installed successfully, you can go ahead and install Pyhive using pip:

pip install pyhive

Fixing  sasl error – fatal error: sasl/sasl.h while installing 

If you are configuring it for the first time, then it is likely that you may get a sasl.h error when installing the sasl module. Install libsasl2-dev module to get rid of errors.

sudo apt-get install libsasl2-dev

Accessing Hive using Python program :

from pyhive import hive
import pandas as pd
conn = hive.Connection(host='localhost',port=10000,database=’database_name’)
df = pd.read_sql("SHOW Databases",conn)
df.show()

after that go through the python file and perform queries
